
class SpeechRecognizer(object):
    def __init__(self, model_path):
        self.session = tf.Session()
        tf.saved_model.loader.load(self.session, [tf.saved_model.tag_constants.SERVING], model_path)
        self.inputs = self.session.graph.get_tensor_by_name('inputs:0')
        self.seq_len = self.session.graph.get_tensor_by_name('seq_len:0')

        self.decoded_dense_tensor = self.session.graph.get_tensor_by_name('decoded_dense:0')

        # Результат декодирования сетью из графа вычислений в виде sparse тензора
        self.decoded_sparse_tensor_indices = self.session.graph.get_tensor_by_name('CTCGreedyDecoder:0')
        self.decoded_sparse_tensor_values = self.session.graph.get_tensor_by_name('CTCGreedyDecoder:1')
        self.decoded_sparse_tensor_dense_shape = self.session.graph.get_tensor_by_name('CTCGreedyDecoder:2')

    def predict_by_audio_mfcc(self, audio_mfcc):
        features = np.asarray([audio_mfcc])
        sequence_len = [len(audio_mfcc)]

        feed = {self.inputs: features, self.seq_len: sequence_len}

        indices, values, dense_shape = self.session.run(
            [self.decoded_sparse_tensor_indices, self.decoded_sparse_tensor_values,
             self.decoded_sparse_tensor_dense_shape], feed_dict=feed)

        return decode_sparse(indices, values, dense_shape)[0]


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
