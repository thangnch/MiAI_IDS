from kafka import KafkaConsumer
import time
import pickle
import numpy as np
import pandas as pd
from io import StringIO

# Tải mô hình từ tệp pickle
with open("random_forest_model.pkl", 'rb') as file:
    model = pickle.load(file)

# Khai báo thông tin kafka
consumer = KafkaConsumer(
    'network_attack',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='your_consumer_group'
)

# Cấu hình tham số kafka
batch_size = 10 # Đủ 10 bản ghi là bắt đầu xử lý
timeout_readkafka = 5000 # Đợi bản ghi trên kafka 5 giây
batch_wait_time = 60000 # 60s giây, đợi 60 giây mà ko đủ 10 bản ghi thì có bao nhiêu predict bây nhiều
hack_threshold = 0.5

column_names = [
    'duration', 'protocol_type', 'service', 'flag', 'src_bytes', 'dst_bytes',
    'land', 'wrong_fragment', 'urgent', 'hot', 'num_failed_logins', 'logged_in',
    'num_compromised', 'root_shell', 'su_attempted', 'num_root',
    'num_file_creations', 'num_shells', 'num_access_files', 'num_outbound_cmds',
    'is_host_login', 'is_guest_login', 'count', 'srv_count', 'serror_rate',
    'srv_serror_rate', 'rerror_rate', 'srv_rerror_rate', 'same_srv_rate',
    'diff_srv_rate', 'srv_diff_host_rate', 'dst_host_count', 'dst_host_srv_count',
    'dst_host_same_srv_rate', 'dst_host_diff_srv_rate', 'dst_host_same_src_port_rate',
    'dst_host_srv_diff_host_rate', 'dst_host_serror_rate', 'dst_host_srv_serror_rate',
    'dst_host_rerror_rate', 'dst_host_srv_rerror_rate', 'attack', 'level'
]


def get_train_encoded_features():
    train_file_small = 'KDDTrain+.txt'
    df = pd.read_csv(train_file_small)

    df.columns = column_names
    df['attack_flag'] = df['attack'].apply(lambda x: 0 if x == 'normal' else 1)

    # Mã hóa các đặc trưng phân loại
    categorical_features = ['protocol_type', 'service', 'flag']
    encoded_features = pd.get_dummies(df[categorical_features])

    # # Các đặc trưng số
    # numeric_features = ['duration', 'src_bytes', 'dst_bytes']
    # # Chuẩn bị dữ liệu cho mô hình
    # dataset_prepared = encoded_features.join(df[numeric_features])
    return encoded_features  # dataset_prepared, df, encoded_features


encoded_features = get_train_encoded_features()

def process_incoming_df(income_df, train_encoded_features):
    # Assign columns names
    income_df.columns = column_names

    # Assign attach_flag
    income_df['attack_flag'] = 0

    # One hot transform
    categorical_features = ['protocol_type', 'service', 'flag']
    test_encoded_base = pd.get_dummies(income_df[categorical_features])

    # Add thêm các cột giá trị không có trong test (dựa vào full data của train)
    test_index = np.arange(len(income_df))
    missing_columns = list(set(train_encoded_features.columns) - set(test_encoded_base.columns))
    test_missing_df = pd.DataFrame(0, index=test_index, columns=missing_columns)
    test_encoded_aligned = test_encoded_base.join(test_missing_df)
    test_encoded_final = test_encoded_aligned[train_encoded_features.columns].fillna(0)

    # Thêm Các đặc trưng số
    numeric_features = ['duration', 'src_bytes', 'dst_bytes']
    test_data_prepared = test_encoded_final.join(income_df[numeric_features])

    return test_data_prepared

def process_batch(batch):
    # Lặp qua từng message trong batch
    df_string = ""
    for i_message in batch:
        df_string += str(i_message.value.decode("utf-8"))

    print("DF String =" , df_string)

    df = pd.read_csv(StringIO(df_string), sep=",", header=None)
    print("Số lượng bản ghi đọc được = ", len(df))

    df = process_incoming_df(df, encoded_features)

    predictions = model.predict(df)
    ratio  = np.count_nonzero(predictions)/len(predictions)
    print("Ratio = ", ratio)
    if ratio > hack_threshold:
        print("🛑HACKED!!!!!" * 20)
        return True
    else:
        print("✅I AM SAFE!!!!!" * 20)
        return False


batch = []
start_time = time.time()
print("Start listening...")
while True:
    # Đọc từ kafka
    records = consumer.poll(timeout_ms=timeout_readkafka)

    if not records:
        continue

    for topic_partition, messages in records.items():
        # Đưa vào batch
        for message in messages:
            batch.append(message)
            # Nếu đủ 10 bản ghi hoặc hết 1 phút thì dự đoán
            if len(batch) >= batch_size or (time.time()-start_time)*1000>=batch_wait_time:
                # Xử lý
                consumer.commit()
                process_batch(batch)

                batch = []
                start_time = time.time()

