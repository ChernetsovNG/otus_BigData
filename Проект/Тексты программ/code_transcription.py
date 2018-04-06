
space_token = '<space>'
empty_token = '<empty>'
space_index = 0
alphabet_chars = set("абвгдеёжзийклмнопрстуфхцчшщъыьэюя")
all_chars = [' ']  # ставим пробел на первое место, чтобы у него был нулевой индекс (по соглашению)
all_chars.extend(sorted(alphabet_chars))
all_chars.append(empty_token)  # добавляем пустой символ в конец
char_to_indices = {char: index for index, char in enumerate(all_chars)}
indices_to_chars = {index: char for char, index in char_to_indices.items()}
num_classes = len(char_to_indices)


def decode_indexes_to_string(y_batch_origin):
    result = []
    for line in y_batch_origin:
        result.append(''.join(indices_to_chars[x] for x in line))
    return result


def decode_sparse(sparse_tensor):
    indices = sparse_tensor.indices
    values = sparse_tensor.values
    dense_shape = sparse_tensor.dense_shape
    return decode_sparse(indices, values, dense_shape)


def decode_sparse(indices, values, dense_shape):
    batch_size = dense_shape[0]
    ans = np.zeros(shape=dense_shape, dtype=int)
    seq_lengths = np.zeros(shape=(batch_size,), dtype=np.int)
    # print("d values: {}".format(d.values))
    for ind, val in zip(indices, values):
        ans[ind[0], ind[1]] = val
        seq_lengths[ind[0]] = max(seq_lengths[ind[0]], ind[1] + 1)
    result = []
    for i in range(batch_size):
        str_encoded = ans[i, :seq_lengths[i]]
        str_decoded = "".join(map(lambda index: indices_to_chars[index], str_encoded))
        str_decoded = remove_duplicate_spaces(str_decoded.replace(empty_token, ''))
        result.append(str_decoded)
    return result


def remove_duplicate_spaces(text):
    return ' '.join(text.split())


def encode_text(text):
    targets = text.replace(' ', '  ')
    targets = targets.split(' ')
    targets_clear = []
    for target in targets:
        target_clear = "".join([x for x in target if x in alphabet_chars])
        targets_clear.append(target_clear)

    # Добавляем пустую метку
    targets = np.hstack([space_token if x == '' else list(x) for x in targets_clear])

    # Преобразуем символы в индексы
    coded_targets = np.asarray([char_to_indices[' '] if x == space_token
                                else char_to_indices[x] for x in targets])

    return coded_targets
    