import pandas as pd
import json
import argparse
import sys
import os
import psycopg2
import psycopg2.extras
from pathlib import Path
from tqdm import tqdm
import io
import uuid
from datetime import datetime, timedelta
import random

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

def analyze_source_data(records):
    """Анализирует исходные данные для генерации синтетических записей"""
    analysis = {
        'processes': set(),
        'chain_lengths': [],
        'timestamps': [],
        'probabilities': [],
        'anomaly_scores': [],
        'hosts': []
    }
    
    for record in records:
        # Собираем процессы из цепочек
        chain_data = record.get('chain_proc_names') or record.get('chain_proc_info')
        if chain_data:
            chain = parse_chain_data(chain_data)
            if chain:
                analysis['processes'].update(chain)
                analysis['chain_lengths'].append(len(chain))
        
        # Собираем таймстампы
        ts = record.get('last_changed') or record.get('_last_changed')
        if ts:
            try:
                dt = pd.to_datetime(ts)
                analysis['timestamps'].append(dt)
            except:
                pass
        
        # Собираем хосты
        host = record.get('host')
        if host:
            analysis['hosts'].append(host)
        
        # Собираем probability (из step)
        step = record.get('step')
        if step is not None:
            try:
                prob = min(float(step) / 100.0, 1.0)
                analysis['probabilities'].append(prob)
            except:
                pass
        
        # Собираем anomaly_score (из time_like_number)
        tln = record.get('time_like_number')
        if tln is not None:
            try:
                score = abs((float(tln) / 1000000.0) % 1.0)
                analysis['anomaly_scores'].append(score)
            except:
                pass
    
    # Конвертируем set в list для JSON сериализации
    analysis['processes'] = list(analysis['processes']) if analysis['processes'] else ['procA', 'procB', 'procC', 'procD', 'procE']
    analysis['chain_lengths'] = analysis['chain_lengths'] if analysis['chain_lengths'] else [2, 3, 4]
    analysis['hosts'] = list(set(analysis['hosts'])) if analysis['hosts'] else ['host1', 'host2', 'host3']
    
    return analysis

def generate_synthetic_records(analysis, count, existing_records):
    """Генерирует синтетические записи на основе анализа исходных данных"""
    synthetic = []
    
    # Определяем диапазоны для генерации
    min_chain_len = min(analysis['chain_lengths']) if analysis['chain_lengths'] else 2
    max_chain_len = max(analysis['chain_lengths']) if analysis['chain_lengths'] else 4
    
    if len(analysis['timestamps']) >= 2:
        min_date = min(analysis['timestamps'])
        max_date = max(analysis['timestamps'])
    else:
        min_date = pd.Timestamp('2024-01-01')
        max_date = pd.Timestamp('2024-12-31')
    
    time_diff_seconds = int((max_date - min_date).total_seconds())
    
    # Используем существующие записи как шаблоны для вариативности
    template_records = existing_records[-100:] if len(existing_records) >= 10 else existing_records
    
    print(f" Генерация {count} синтетических записей на основе {len(existing_records)} реальных...")
    
    for i in range(count):
        # Выбираем случайную запись как шаблон (если есть)
        if template_records:
            template = random.choice(template_records)
            base_chain = template.get('sequence', [])
            base_host = template.get('host', 'unknown')
        else:
            base_chain = []
            base_host = 'unknown'
        
        # Генерируем случайную цепочку
        if analysis['processes']:
            chain_len = random.randint(min_chain_len, max(max_chain_len, min_chain_len + 1))
            if base_chain and len(base_chain) > 1:
                # Вариация существующей цепочки: случайно добавляем/удаляем процессы
                chain = base_chain[:random.randint(1, len(base_chain))]
                available_processes = [p for p in analysis['processes'] if p not in chain]
                if available_processes and random.random() > 0.5:
                    chain.append(random.choice(available_processes))
            else:
                chain = random.sample(analysis['processes'], min(chain_len, len(analysis['processes'])))
        else:
            chain = ['procA', 'procB', 'procC']
        
        # Генерируем timestamp в диапазоне исходных данных
        if time_diff_seconds > 0:
            random_seconds = random.randint(0, time_diff_seconds)
            timestamp = (min_date + timedelta(seconds=random_seconds)).strftime('%Y-%m-%d %H:%M:%S')
        else:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Генерируем probability (с небольшим случайным отклонением от шаблона)
        if analysis['probabilities']:
            base_prob = random.choice(analysis['probabilities'])
        else:
            base_prob = random.uniform(0.3, 1.0)
        probability = max(0.0, min(1.0, base_prob + random.uniform(-0.1, 0.1)))
        
        # Генерируем anomaly_score
        if analysis['anomaly_scores']:
            base_score = random.choice(analysis['anomaly_scores'])
        else:
            base_score = random.uniform(0.0, 0.5)
        anomaly_score = max(0.0, min(1.0, base_score + random.uniform(-0.05, 0.05)))
        
        # Генерируем host
        if analysis['hosts']:
            host = random.choice(analysis['hosts'])
        else:
            host = base_host if base_host != 'unknown' else f"host_{random.randint(1, 10)}"
        
        record = {
            'trace_id': f"synth_{uuid.uuid4()}",
            'timestamp': timestamp,
            'host': host,
            'sequence': chain,
            'probability': round(probability, 4),
            'anomaly_score': round(anomaly_score, 4),
            'sequence_str': ' -> '.join(chain)
        }
        
        synthetic.append(record)
    
    return synthetic

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

def convert_to_ml_format(records, desired_count=None):
    """Преобразует записи в ML-формат с возможностью генерации синтетических данных"""
    processed_records = []
    trace_id_counts = {}  # Счетчик для отслеживания дубликатов proc_id
    
    # Определяем сколько реальных записей обработать
    if desired_count is not None and desired_count < len(records):
        records_to_process = records[:desired_count]
    else:
        records_to_process = records
    
    # Обрабатываем реальные записи
    for idx, record in enumerate(tqdm(records_to_process, desc="Конвертация в ML-формат", unit="rec")):
        ml_record = {
            'trace_id': None,
            'timestamp': None,
            'host': None,
            'sequence': [],
            'probability': 1.0,
            'anomaly_score': 0.0,
            'sequence_str': ""
        }
        
        # Генерация уникального trace_id
        if record.get('proc_id') is not None:
            base_trace_id = f"proc_{record['proc_id']}"
            count = trace_id_counts.get(base_trace_id, 0)
            if count == 0:
                ml_record['trace_id'] = base_trace_id
            else:
                ml_record['trace_id'] = f"{base_trace_id}_{count}"
            trace_id_counts[base_trace_id] = count + 1
        else:
            ml_record['trace_id'] = str(uuid.uuid4())
        
        # Остальная часть функции остается без изменений...
        # [Вставьте остальной код функции сюда]
        
        # host
        ml_record['host'] = record.get('host', 'unknown')
        
        # timestamp
        if record.get('last_changed') is not None:
            ml_record['timestamp'] = record['last_changed']
        else:
            ml_record['timestamp'] = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        
        # sequence
        chain_data = record.get('chain_proc_names') or record.get('chain_proc_info')
        ml_record['sequence'] = parse_chain_data(chain_data)
        
        if not ml_record['sequence']:
            proc_name = record.get('proc_name', '')
            parent_name = record.get('parent_proc_name', '')
            if proc_name and parent_name:
                ml_record['sequence'] = [parent_name, proc_name]
            elif proc_name:
                ml_record['sequence'] = [proc_name]
        
        # probability
        if record.get('step') is not None:
            try:
                ml_record['probability'] = min(float(record['step']) / 100.0, 1.0)
            except:
                ml_record['probability'] = 0.5 + (idx % 10) * 0.05
        else:
            ml_record['probability'] = 0.3 + (idx % 70) * 0.01
        
        # anomaly_score
        if record.get('time_like_number') is not None:
            try:
                tln = float(record['time_like_number'])
                ml_record['anomaly_score'] = abs((tln / 1000000.0) % 1.0)
            except:
                ml_record['anomaly_score'] = (idx % 100) / 100.0
        else:
            ml_record['anomaly_score'] = (idx % 100) / 100.0
        
        # sequence_str
        ml_record['sequence_str'] = ' -> '.join(ml_record['sequence']) if ml_record['sequence'] else ""
        
        processed_records.append(ml_record)
    
    # Генерируем синтетические записи если нужно
    if desired_count is not None and len(processed_records) < desired_count:
        additional = desired_count - len(processed_records)
        analysis = analyze_source_data(records)
        synthetic = generate_synthetic_records(analysis, additional, processed_records)
        processed_records.extend(synthetic)
    
    return processed_records

def xlsx_to_ml_json(xlsx_path, json_path=None, save_to_file=True, desired_count=None):
    """Конвертирует XLSX в JSON в ML-формате с возможностью генерации синтетических данных"""
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
        
        # Конвертируем в ML-формат с заданным количеством
        ml_records = convert_to_ml_format(records, desired_count)
        
        if save_to_file:
            if json_path is None:
                json_path = str(Path(xlsx_path).stem + '_ml.json')
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(ml_records, f, ensure_ascii=False, indent=2)
            
            print(f"✅ ML-JSON сохранён: {json_path} ({len(ml_records):,} записей)")
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
        
        # Создаем таблицу, если её нет
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
    parser = argparse.ArgumentParser(description='Загрузчик ML-данных в PostgreSQL кластер с генерацией синтетических записей')
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
    parser.add_argument('--count', '-c', type=int, help='Желаемое количество записей. Если больше чем в XLSX, будут сгенерированы синтетические записи на основе шаблонов')
    
    args = parser.parse_args()
    
    # Конвертируем в ML-формат
    print("\n" + "="*60)
    print("ШАГ 1: Конвертация XLSX в ML-формат")
    if args.count:
        print(f"   Желаемое количество записей: {args.count:,}")
    print("="*60)
    
    _, ml_records = xlsx_to_ml_json(args.file, save_to_file=True, desired_count=args.count)
    
    if args.dry_run:
        print("\n✅ Dry-run завершен. Данные не загружены в БД.")
        print(f"   Всего создано записей: {len(ml_records):,}")
        print(f"   Реальных записей: {min(args.count or len(ml_records), len(pd.read_excel(args.file)))}")
        if args.count and args.count > len(pd.read_excel(args.file)):
            print(f"   Синтетических записей: {args.count - len(pd.read_excel(args.file))}")
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
    if args.count and success_count != args.count:
        print(f"⚠️  Запрошено: {args.count:,} записей")
    print("="*60)

if __name__ == '__main__':

    main()
