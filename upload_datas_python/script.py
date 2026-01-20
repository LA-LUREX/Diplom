#!/usr/bin/env python3
"""
ML Data Loader for PostgreSQL Cluster (FIXED for empty timestamps, chain parsing, and duplicate proc_id)
"""

import pandas as pd
import json
import argparse
import sys
import os
import psycopg2
import psycopg2.extras
import random
from pathlib import Path
from tqdm import tqdm
import io
import uuid
from datetime import datetime
from collections import Counter  # Для проверки дубликатов

# Константа для преобразования Excel-даты в datetime
EXCEL_EPOCH = pd.Timestamp('1900-01-01')

def excel_date_to_datetime(excel_date):
    """Преобразует Excel serial date в datetime строку формата YYYY-MM-DD HH:MM:SS"""
    try:
        if pd.isna(excel_date) or excel_date == '':
            return None
        # Excel считает 1900 год високосным (что неверно), поэтому корректируем
        if excel_date > 59:
            excel_date -= 1
        dt = EXCEL_EPOCH + pd.Timedelta(days=excel_date - 2)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return None

def parse_chain_data(chain_data):
    """Парсит цепочку процессов из различных форматов"""
    if not chain_data or pd.isna(chain_data):
        return []
    
    # Если это уже список (из JSON)
    if isinstance(chain_data, list):
        return chain_data
    
    # Если это строка
    if isinstance(chain_data, str):
        # Пробуем разобрать как JSON-массив
        chain_data = chain_data.strip()
        if (chain_data.startswith('[') and chain_data.endswith(']')) or \
           (chain_data.startswith('"[') and chain_data.endswith(']"')):
            try:
                return json.loads(chain_data)
            except:
                pass
        
        # Убираем квадратные скобки, если есть
        chain_data = chain_data.strip('[]')
        
        # Обрабатываем разделители в порядке приоритета
        for separator in ['←', '←', ',', ';']:  # ← (U+2190) и ← (U+2190)
            if separator in chain_data:
                return [x.strip() for x in chain_data.split(separator) if x.strip()]
        
        # Если нет разделителей, возвращаем как один элемент
        return [chain_data] if chain_data else []
    
    # Для всех остальных типов
    return [str(chain_data)]

def check_duplicates(records):
    """Проверяет дубликаты proc_id в исходных данных и выводит статистику"""
    proc_ids = []
    for record in records:
        proc_id = record.get('proc_id')
        if proc_id is not None:
            proc_ids.append(proc_id)
    
    duplicates = {pid: count for pid, count in Counter(proc_ids).items() if count > 1}
    
    if duplicates:
        print(f"⚠️  Найдено {len(duplicates)} дублирующихся proc_id:")
        # Показываем первые 10 дубликатов
        for pid, count in list(duplicates.items())[:10]:
            print(f"   proc_id={pid}: повторяется {count} раз")
        if len(duplicates) > 10:
            print(f"   ... и еще {len(duplicates) - 10} дубликатов")
        print(f"   Всего {len(proc_ids) - len(set(proc_ids))} дублирующихся записей")
    else:
        print("✅ Дубликатов proc_id не найдено")
    
    return bool(duplicates)

def convert_to_ml_format(records):
    """Преобразует записи в ML-формат с гарантированно уникальными trace_id"""
    processed_records = []
    used_proc_ids = set()  # Отслеживаем использованные proc_id для обнаружения дубликатов
    
    for idx, record in enumerate(tqdm(records, desc="Конвертация в ML-формат", unit="rec")):
        ml_record = {
            'trace_id': None,
            'timestamp': None,
            'host': 'unknown',
            'sequence': [],
            'probability': 1.0,
            'anomaly_score': 0.0,
            'sequence_str': ""
        }
        
        # === ИСПРАВЛЕНИЕ: Обработка дубликатов proc_id ===
        proc_id = record.get('proc_id')
        if proc_id is not None:
            base_trace_id = f"proc_{proc_id}"
            if base_trace_id in used_proc_ids:
                # Дубликат найден - генерируем UUID
                ml_record['trace_id'] = str(uuid.uuid4())
                # Показываем предупреждение для первых 5 дубликатов (чтобы не спамить)
                if sum(1 for k in used_proc_ids if k.startswith('proc_')) < 5:
                    print(f"⚠️  Дубликат proc_id={proc_id}, использован UUID: {ml_record['trace_id'][:8]}...")
            else:
                ml_record['trace_id'] = base_trace_id
                used_proc_ids.add(base_trace_id)
        else:
            ml_record['trace_id'] = str(uuid.uuid4())
        
        # Заполняем host
        ml_record['host'] = record.get('host', 'unknown') or 'unknown'
        
        # Заполняем timestamp
        if record.get('last_changed') is not None:
            ml_record['timestamp'] = record['last_changed']
        else:
            # Генерируем случайное время в 2024 году для тестов
            ml_record['timestamp'] = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        
        # Заполняем sequence из chain_proc_names или chain_proc_info
        chain_data = record.get('chain_proc_names') or record.get('chain_proc_info')
        ml_record['sequence'] = parse_chain_data(chain_data)
        
        # Если sequence пустой, пытаемся создать из proc_name и parent_proc_name
        if not ml_record['sequence']:
            proc_name = record.get('proc_name', '')
            parent_name = record.get('parent_proc_name', '')
            if proc_name and parent_name:
                ml_record['sequence'] = [parent_name, proc_name]
            elif proc_name:
                ml_record['sequence'] = [proc_name]
        
        # Заполняем probability
        if record.get('step') is not None:
            try:
                ml_record['probability'] = min(float(record['step']) / 100.0, 1.0)
            except:
                ml_record['probability'] = 0.5 + (idx % 10) * 0.05  # Генерируем разные значения
        else:
            # Генерируем случайное значение от 0.3 до 1.0
            ml_record['probability'] = 0.3 + (idx % 70) * 0.01
        
        # Заполняем anomaly_score
        if record.get('time_like_number') is not None:
            try:
                tln = float(record['time_like_number'])
                # Нормализуем к диапазону [0,1] через синус для разнообразия
                ml_record['anomaly_score'] = abs((tln / 1000000.0) % 1.0)
            except:
                ml_record['anomaly_score'] = (idx % 100) / 100.0  # 0.0 to 0.99
        else:
            ml_record['anomaly_score'] = (idx % 100) / 100.0
        
        # Создаем sequence_str
        ml_record['sequence_str'] = ' -> '.join(ml_record['sequence']) if ml_record['sequence'] else ""
        
        processed_records.append(ml_record)
    
    return processed_records

def xlsx_to_json(xlsx_path, json_path=None):
    """Конвертирует XLSX в JSON с обработкой Timestamp и NaN"""
    try:
        print(f"📄 Чтение файла: {xlsx_path}")
        
        # Читаем XLSX
        df = pd.read_excel(
            xlsx_path, 
            na_values=['', 'NaN', 'NULL', 'null', '#N/A'],
            keep_default_na=True
        )
        
        # Преобразуем _last_changed в last_changed
        if '_last_changed' in df.columns:
            print("🔧 Преобразую _last_changed в last_changed...")
            df['last_changed'] = df['_last_changed'].apply(excel_date_to_datetime)
            df.drop(columns=['_last_changed'], inplace=True)
        
        # Обрабатываем Timestamp → строка ISO (или None)
        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]':
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').where(df[col].notna(), None)
        
        # Удаляем полностью пустые строки и заменяем NaN на None
        df = df.dropna(how='all')
        df = df.where(pd.notnull(df), None)
        
        # Заменяем пустые строки на None для timestamp-like колонок
        for col in ['last_changed', 'last_event_uuid', 'proc_meta', 'proc_hash']:
            if col in df.columns:
                df[col] = df[col].replace('', None)
        
        records = df.to_dict(orient='records')
        
        if json_path is None:
            json_path = str(Path(xlsx_path).with_suffix('.json'))
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        
        print(f"✅ JSON сохранён: {json_path} ({len(records):,} записей)")
        return json_path, records
    
    except Exception as e:
        print(f"❌ Ошибка конвертации: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def xlsx_to_ml_json(xlsx_path, json_path=None, save_to_file=True):
    """Конвертирует XLSX в JSON в ML-формате с обработкой дубликатов"""
    try:
        print(f"📄 Чтение XLSX для ML-конвертации: {xlsx_path}")
        
        # Читаем XLSX
        df = pd.read_excel(
            xlsx_path, 
            na_values=['', 'NaN', 'NULL', 'null', '#N/A'],
            keep_default_na=True
        )
        
        # Преобразуем _last_changed в last_changed
        if '_last_changed' in df.columns:
            print("🔧 Преобразую _last_changed в last_changed...")
            df['last_changed'] = df['_last_changed'].apply(excel_date_to_datetime)
            df.drop(columns=['_last_changed'], inplace=True)
        
        # Очищаем данные
        df = df.dropna(how='all')
        df = df.where(pd.notnull(df), None)
        
        records = df.to_dict(orient='records')
        
        # === Проверяем дубликаты ===
        print("\n" + "="*50)
        print("Проверка дубликатов proc_id...")
        print("="*50)
        has_duplicates = check_duplicates(records)
        
        # Конвертируем в ML-формат
        ml_records = convert_to_ml_format(records)
        
        if save_to_file:
            if json_path is None:
                json_path = str(Path(xlsx_path).stem + '_ml.json')
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(ml_records, f, ensure_ascii=False, indent=2)
            
            print(f"\n✅ ML-JSON сохранён: {json_path} ({len(ml_records):,} записей)")
            if has_duplicates:
                print("✅ Дубликаты были обработаны с помощью UUID")
            return json_path, ml_records
        else:
            return None, ml_records
    
    except Exception as e:
        print(f"❌ Ошибка ML-конвертации: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def load_ml_to_postgresql(records, db_config, batch_size=5000, truncate=False):
    """Загружает ML-данные в таблицу ml_process_logs_modify"""
    try:
        print(f"🔗 Подключение к PostgreSQL: {db_config['host']}:{db_config['port']}")
        
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Проверяем, существует ли таблица
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'ml_process_logs_modify'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        # Создаем таблицу, если её нет (НЕ удаляем существующую!)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS ml_process_logs_modify (
            trace_id TEXT PRIMARY KEY,
            timestamp TIMESTAMP,
            host TEXT,
            sequence JSONB,
            probability REAL,
            anomaly_score REAL,
            sequence_str TEXT,
            loaded_at TIMESTAMP DEFAULT NOW()
        );
        """
        
        cursor.execute(create_table_sql)
        
        # Создаем индексы, если они не существуют
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ml_mod_timestamp ON ml_process_logs_modify(timestamp);
            CREATE INDEX IF NOT EXISTS idx_ml_mod_probability ON ml_process_logs_modify(probability);
            CREATE INDEX IF NOT EXISTS idx_ml_mod_anomaly ON ml_process_logs_modify(anomaly_score);
            CREATE INDEX IF NOT EXISTS idx_ml_mod_host ON ml_process_logs_modify(host);
        """)
        
        if truncate:
            cursor.execute("TRUNCATE ml_process_logs_modify CASCADE;")
            print("🗑️  Таблица ml_process_logs_modify очищена")
        
        conn.commit()
        
        print(f"📤 Загрузка ML-данных через COPY (batch: {batch_size})...")
        
        # Подготавливаем данные
        buffer = io.StringIO()
        success_count = 0
        
        for record in tqdm(records, desc="Обработка ML-записей", unit="rec"):
            try:
                values = []
                
                # trace_id
                values.append(str(record.get('trace_id', '')) or '\\N')
                
                # timestamp
                timestamp = record.get('timestamp')
                values.append(timestamp if timestamp else '\\N')
                
                # host
                host = record.get('host', 'unknown')
                values.append(host if host else '\\N')
                
                # sequence (JSONB)
                sequence = record.get('sequence', [])
                values.append(json.dumps(sequence) if sequence else '\\N')
                
                # probability
                prob = record.get('probability', 1.0)
                values.append(str(prob) if prob is not None else '\\N')
                
                # anomaly_score
                score = record.get('anomaly_score', 0.0)
                values.append(str(score) if score is not None else '\\N')
                
                # sequence_str
                seq_str = record.get('sequence_str', '')
                values.append(seq_str if seq_str else '\\N')
                
                line = '\t'.join(values)
                buffer.write(line + '\n')
                success_count += 1
            except Exception as e:
                print(f"⚠️  Ошибка в записи {record.get('trace_id')}: {e}")
                continue
        
        buffer.seek(0)
        
        # COPY
        cursor.copy_from(
            buffer,
            'ml_process_logs_modify',
            null='\\N',
            columns=['trace_id', 'timestamp', 'host', 'sequence', 'probability', 'anomaly_score', 'sequence_str']
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Успешно загружено {success_count:,} из {len(records):,} ML-записей в ml_process_logs_modify")
        
        if success_count != len(records):
            print(f"⚠️  {len(records) - success_count} записей не были загружены из-за ошибок")
        
        return success_count
        
    except Exception as e:
        print(f"❌ Ошибка загрузки ML-данных: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def verify_data_in_db(db_config, table_name='ml_process_logs_modify', limit=5):
    """Проверяет, что данные загрузились в БД"""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        print(f"\n📊 В таблице {table_name} найдено {count:,} записей")
        
        if count > 0:
            print(f"\nПервые {limit} записей:")
            cursor.execute(f"SELECT trace_id, timestamp, host, probability, anomaly_score, sequence_str FROM {table_name} ORDER BY timestamp DESC LIMIT {limit}")
            rows = cursor.fetchall()
            for row in rows:
                print(f"  trace_id: {row[0]}")
                print(f"  timestamp: {row[1]}")
                print(f"  host: {row[2]}")
                print(f"  probability: {row[3]}")
                print(f"  anomaly_score: {row[4]}")
                print(f"  sequence_str: {row[5]}")
                print("  ---")
        
        cursor.close()
        conn.close()
        return count
        
    except Exception as e:
        print(f"❌ Ошибка при проверке данных: {e}")
        return -1

def main():
    parser = argparse.ArgumentParser(description='Загрузчик ML-данных в PostgreSQL кластер')
    parser.add_argument('--file', '-f', required=True, help='Путь к XLSX файлу')
    parser.add_argument('--db-host', default='10.0.2.12', help='Хост PostgreSQL мастера')
    parser.add_argument('--db-port', type=int, default=5432, help='Порт PostgreSQL')
    parser.add_argument('--db-name', default='postgres', help='Имя БД')
    parser.add_argument('--db-user', default='dbadmin', help='Пользователь БД')
    parser.add_argument('--db-pass', help='Пароль БД (если не указан, запросит)')
    parser.add_argument('--batch-size', type=int, default=5000, help='Размер batch')
    parser.add_argument('--truncate', action='store_true', help='Очистить таблицу перед загрузкой')
    parser.add_argument('--dry-run', action='store_true', help='Только конвертация, без загрузки в БД')
    parser.add_argument('--verify', action='store_true', help='Проверить данные в БД после загрузки')
    
    args = parser.parse_args()
    
    # Конвертируем в ML-формат
    print("\n" + "="*60)
    print("ШАГ 1: Конвертация XLSX в ML-формат")
    print("="*60)
    
    _, ml_records = xlsx_to_ml_json(args.file, save_to_file=True)
    
    if args.dry_run:
        print("\n✅ Dry-run завершен. Данные не загружены в БД.")
        print(f"   Создано {len(ml_records)} ML-записей")
        sys.exit(0)
    
    # Загружаем в БД
    print("\n" + "="*60)
    print("ШАГ 2: Загрузка в PostgreSQL")
    print("="*60)
    
    if not args.db_pass:
        args.db_pass = input(f"🔐 Введите пароль для {args.db_user}@{args.db_host}: ")
    
    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_pass,
        'connect_timeout': 10
    }
    
    # Проверяем подключение
    try:
        conn = psycopg2.connect(**db_config)
        conn.close()
        print("✅ Подключение к PostgreSQL успешно")
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        sys.exit(1)
    
    # Загружаем данные
    success_count = load_ml_to_postgresql(ml_records, db_config, args.batch_size, args.truncate)
    
    # Проверяем загрузку
    if args.verify:
        print("\n" + "="*60)
        print("ШАГ 3: Проверка данных в БД")
        print("="*60)
        verify_data_in_db(db_config, 'ml_process_logs_modify')
    
    print("\n" + "="*60)
    print(f"🎉 Готово! Данные доступны в таблице ml_process_logs_modify")
    print(f"📊 Мастер: {args.db_host}")
    print(f"📈 Успешно загружено: {success_count:,} записей")
    if success_count != len(ml_records):
        print(f"⚠️  Пропущено записей: {len(ml_records) - success_count}")
    print("="*60)

if __name__ == '__main__':
    main()