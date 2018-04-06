
def load_model_from_checkpoint_and_save_as_saved_model(checkpoint_dir, saved_model_dir):
    hidden_units = 128
    graph = tf.Graph()
    with graph.as_default():
        tf.add_check_numerics_ops()

        inputs = tf.placeholder(tf.float32, shape=[None, None, num_features_mfcc], name='inputs')
        targets = tf.sparse_placeholder(tf.int32, name='targets')
        seq_len = tf.placeholder(tf.int32, shape=[None], name='seq_len')

        model = Sequential()
        model.add(
            Bidirectional(CuDNNLSTM(units=hidden_units, return_sequences=True),
                          input_shape=(None, num_features_mfcc), name="bidirectional_1"))
        model.add(
            Bidirectional(CuDNNLSTM(units=hidden_units, return_sequences=True), name="bidirectional_2"))
        model.add(
            Bidirectional(CuDNNLSTM(units=hidden_units, return_sequences=True), name="bidirectional_3"))
        model.add(TimeDistributed(Dense(num_classes), name="time_distributed_1"))

        logits = model(inputs)  # Выход модели
        logits = tf.transpose(logits, [1, 0, 2], name='logits')

        loss = tf.nn.ctc_loss(targets, logits, seq_len, ignore_longer_outputs_than_inputs=True)
        cost = tf.reduce_mean(loss, name='cost')

        # декодируем выход модели (decoded) и сравниваем истинные метки и получившиеся по расстоянию
        # Левенштейна между последовательностями
        # decoded, log_prob = tf.nn.ctc_beam_search_decoder(logits, seq_len)
        decoded, log_prob = tf.nn.ctc_greedy_decoder(logits, seq_len)
        decoded_dense = tf.sparse_tensor_to_dense(decoded[0], default_value=0, name='decoded_dense')
        ler = tf.reduce_mean(tf.edit_distance(tf.cast(decoded[0], tf.int32), targets), name='ler')

    with tf.Session(graph=graph) as session:
        saver = tf.train.Saver(tf.global_variables())
        checkpoint = tf.train.latest_checkpoint(checkpoint_dir=checkpoint_dir)

        if checkpoint is not None:
            print('Loading checkpoint {}'.format(checkpoint))
            try:
                saver.restore(session, checkpoint)
            except ValueError:
                print('Incompatible checkpoint')
        else:
            print('Checkpoint in None')

        # Сохраняем модель для дальнейшего использования
        predict_tensor_inputs_info = tf.saved_model.utils.build_tensor_info(inputs)
        predict_tensor_targets_info = tf.saved_model.utils.build_tensor_info(targets)
        predict_tensor_seq_len_info = tf.saved_model.utils.build_tensor_info(seq_len)

        predict_tensor_cost_info = tf.saved_model.utils.build_tensor_info(cost)
        predict_tensor_decoded_indices_info = tf.saved_model.utils.build_tensor_info(decoded[0].indices)
        predict_tensor_decoded_values_info = tf.saved_model.utils.build_tensor_info(decoded[0].values)
        predict_tensor_decoded_shape_info = tf.saved_model.utils.build_tensor_info(decoded[0].dense_shape)
        predict_tensor_decoded_dense_info = tf.saved_model.utils.build_tensor_info(decoded_dense)
        predict_tensor_ler_info = tf.saved_model.utils.build_tensor_info(ler)

        prediction_signature = tf.saved_model.signature_def_utils.build_signature_def(
            inputs={'inputs': predict_tensor_inputs_info,
                    'seq_len': predict_tensor_seq_len_info},
            outputs={'decoded_dense': predict_tensor_decoded_dense_info,
                     'decoded_sparse_indices': predict_tensor_decoded_indices_info,
                     'decoded_sparse_values': predict_tensor_decoded_values_info,
                     'decoded_sparse_shape': predict_tensor_decoded_shape_info},
            method_name=tf.saved_model.signature_constants.PREDICT_METHOD_NAME)

        legacy_init_op = tf.group(tf.tables_initializer(), name='legacy_init_op')

        builder = tf.saved_model.builder.SavedModelBuilder(export_dir=saved_model_dir)
        builder.add_meta_graph_and_variables(session, [tag_constants.SERVING],
                                             signature_def_map={'recognize': prediction_signature},
                                             legacy_init_op=legacy_init_op)
        builder.save()
