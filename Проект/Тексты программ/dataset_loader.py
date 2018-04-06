
class DatasetLoader(object):
    SAMPLE_RATE = 8000
    LOW_FREQ = 50

    def __init__(self, init_load_features_targets=True, dataset_folder=None,
                 dataset_filename=None, ivoice_folder=None, save_data_folder=None):
        self.save_data_folder = save_data_folder
        if init_load_features_targets:  # загружать ли данные при создании объекта
            dataset = load_obj_from_file(dataset_folder, dataset_filename)
            self.chars = self.get_dataset_chars(dataset)
            self.features, self.targets = self.get_features_and_targets(dataset, dataset_folder, ivoice_folder)

    def get_features_and_targets(self, dataset, dataset_folder, ivoice_folder):
        if os.path.isfile(get_pickle_file_path(dataset_folder, 'X')) and \
                os.path.isfile(get_pickle_file_path(dataset_folder, 'y')):
            logger.info("Files X.pkl and y.pkl already exists. Load them")
            X = load_obj_from_file(dataset_folder, 'X')
            y = load_obj_from_file(dataset_folder, 'y')
        else:
            logger.info("Create files X.pkl and y.pkl with training data")
            X, y = self.convert_dataset_to_train_data(dataset, ivoice_folder)
        self.calculate_input_output_length()
        return X, y

    def calculate_input_output_length(self):
        self.len_count_map = {}
        for target in self.targets:
            length = len(target)
            if length in self.len_count_map:
                self.len_count_map[length] += 1
            else:
                self.len_count_map[length] = 1
        self.max_output_len = np.max([len(elem) for elem in self.targets])

    @staticmethod
    def get_dataset_chars(dataset):
        chars = set()
        for data in dataset:
            text = data[1]
            chars.update(list(text.strip().lower()))
        return chars

    # Составляем массивы данных для обучения
    def convert_dataset_to_train_data(self, dataset, ivoice_folder=None):
        X = []
        y = []

        for data in tqdm(dataset):
            features, coded_target, seq_len, original = self.extract_features_and_targets(data)
            if seq_len != 0:
                X.append(features)
                y.append(coded_target)

        # Обрабатываем данные IVOICE и добавляем их к данным датасета
        if ivoice_folder is not None:
            X_ivoice, y_ivoice, original_transcription = self.get_data_from_transcription_and_audio(ivoice_folder)
            X.extend(X_ivoice)
            y.extend(y_ivoice)

        print("X,y contains {} samples".format(len(X)))

        save_obj_into_file(X, self.save_data_folder, 'X')
        save_obj_into_file(y, self.save_data_folder, 'y')

        return X, y

    def get_data_from_transcription_and_audio(self, data_folder):
        result_features = []
        result_targets = []
        original_transcription = []
        # Ищем в директории файлы с transcription в названии
        for dir_path, dir_names, file_names in os.walk(data_folder):
            for file_name in file_names:
                if 'transcription' in file_name:
                    print("Process the file: {}".format(file_name))
                    file_features, file_targets, file_transcription =\
                        self.process_transcription_file(dir_path, file_name)
                    result_features.extend(file_features)
                    result_targets.extend(file_targets)
                    original_transcription.extend(file_transcription)
        return result_features, result_targets, original_transcription

    def process_transcription_file(self, dir_path, transcription_file_name):
        result_features = []
        result_targets = []
        original_transcription = []
        # Обходим строки файла
        with open(dir_path + '/' + transcription_file_name) as transcription_file:
            content = transcription_file.readlines()
            content = [x.strip() for x in content]
            index = 0
            audio_count = 0
            for line in content:
                line = line.replace('\t', ' ')  # меняем табуляцию на пробел если вдруг её встретим
                line_split = line.split(sep=' ')
                if index == 0:
                    # Удаляем из начала файла указатель на порядок байт
                    line_split = [x.replace('\ufeff', '') for x in line_split]
                if len(line_split) > 1:  # если есть транскрпция
                    str_number = line_split[0]  # номер в строке = номеру аудиофайла
                    wavfile_name = dir_path + '/' + str_number + '.wav'
                    rate, audio = wavfile.read(wavfile_name)
                    text = ' '.join(line_split[1:])
                    features, coded_target, seq_len, original = \
                        self.extract_features_and_targets((audio, text))
                    if seq_len != 0:
                        result_features.append(features)
                        result_targets.append(coded_target)
                        original_transcription.append(original)
                        audio_count += 1
                index += 1
            print("Process {} audio files".format(audio_count))
        return result_features, result_targets, original_transcription

    # Кодируем аудио-сигнал в MFCC а символы - в их индексы в массиве chars
    def extract_features_and_targets(self, data):
        audio = data[0]
        text = data[1]

        original = ' '.join(text.strip().lower().split(' ')) \
            .replace('.', '').replace("'", '').replace('-', '').replace(',', '')

        coded_targets = encode_text(original)

        features, seq_len = extract_mfcc_features_from_audio(audio, sample_rate=self.SAMPLE_RATE,
                                                             num_features=num_features_mfcc, low_freq=self.LOW_FREQ)

        return features, coded_targets, seq_len, original
