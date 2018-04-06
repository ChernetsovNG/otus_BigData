
class SpeechSampleGenerator(object):
    def __init__(self, dataset_loader, batch_size):
        self.dataset_loader = dataset_loader
        self.batch_size = batch_size

    @staticmethod
    def get_batch(X, y, batch_size=16):
        n_samples = len(X)
        perm = np.random.permutation(n_samples)
        for batch_indexes in np.resize(perm, (n_samples // batch_size, batch_size)):
            X_batch = [X[i] for i in batch_indexes]
            y_batch = [y[i] for i in batch_indexes]
            sequence_lengths = list(map(len, X_batch))
            X_batch_padded = np.array(
                list(zip_longest(*X_batch, fillvalue=np.zeros(num_features_mfcc)))).transpose([1, 0, 2])
            # Создаём разреженное представление для y
            train_targets = sparse_tuple_from_list_of_lists(y_batch)
            yield X_batch_padded, sequence_lengths, train_targets, y_batch

    # Отбираем образцы, у которых количество символов ограничено заданным диапазоном
    def get_X_y_by_limits(self, bottom_len_limit, top_len_limit):
        X_train_limit = []
        y_train_limit = []
        # если диапазон не задан, то используем весь датасет
        if bottom_len_limit < 0 and top_len_limit < 0:
            X_train_limit.extend(self.dataset_loader.features)
            y_train_limit.extend(self.dataset_loader.targets)
        else:
            for index, y_sample in enumerate(self.dataset_loader.targets):
                if bottom_len_limit <= len(y_sample) <= top_len_limit:
                    X_train_limit.append(self.dataset_loader.features[index])
                    y_train_limit.append(y_sample)
        return X_train_limit, y_train_limit


# Создаёт разреженное представление. Вход: список списков
def sparse_tuple_from_list_of_lists(sequences, dtype=np.int32):
    indices = []
    values = []

    for n, seq in enumerate(sequences):
        indices.extend(zip([n] * len(seq), range(len(seq))))
        values.extend(seq)

    indices = np.asarray(indices, dtype=np.int32)
    values = np.asarray(values, dtype=dtype)
    shape = np.asarray([len(sequences), np.asarray(indices).max(0)[1] + 1], dtype=np.int32)

    return indices, values, shape
