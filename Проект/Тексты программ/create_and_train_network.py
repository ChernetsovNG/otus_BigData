
def train_network_3layer(dataset_loader, batch_size, bottom_len_limit=0, top_len_limit=50, num_epochs=100,
                         epoch_save_step=10):
    sample_gen = SpeechSampleGenerator(dataset_loader, batch_size=batch_size)

    # Выделяем больше памяти на видеокарте
    config = tf.ConfigProto(allow_soft_placement=True)
    config.gpu_options.allocator_type = 'BFC'
    config.gpu_options.per_process_gpu_memory_fraction = 0.80
    config.gpu_options.allow_growth = True

    hidden_units = 128
    learning_rate = 0.0005
    gradient_max_value = 1e7  # ограничение градиентов при обучении

    graph = tf.Graph()
    with graph.as_default():
        tf.add_check_numerics_ops()

        inputs = tf.placeholder(tf.float32, shape=[None, None, num_features])
        # будем использовать SparseTensor, который требуется ctc_loss
        targets = tf.sparse_placeholder(tf.int32)  # истинные метки выхода
        # 1d массив размера [batch_size]
        seq_len = tf.placeholder(tf.int32, shape=[None])

        model = Sequential()
        model.add(
            Bidirectional(CuDNNLSTM(units=hidden_units, return_sequences=True),
                          input_shape=(None, num_features), name="bidirectional_1"))
        model.add(
            Bidirectional(CuDNNLSTM(units=hidden_units, return_sequences=True), name="bidirectional_2"))
        model.add(
            Bidirectional(CuDNNLSTM(units=hidden_units, return_sequences=True), name="bidirectional_3"))
        model.add(TimeDistributed(Dense(num_classes), name="time_distributed_1"))
        model.summary()

        logits = model(inputs)  # Выход модели
        # Time major; нужен тензор вида [max_time, batch_size, num_classes]
        logits = tf.transpose(logits, [1, 0, 2])

        loss = tf.nn.ctc_loss(targets, logits, seq_len, ignore_longer_outputs_than_inputs=True)
        cost = tf.reduce_mean(loss)

        optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)
        # optimizer = tf.train.MomentumOptimizer(learning_rate=learning_rate, momentum=0.001, use_nesterov=False)

        gvs = optimizer.compute_gradients(cost)
        # ограничиваем градиенты, чтобы не уходили в бесконечность
        capped_gvs = [(tf.clip_by_value(grad, -gradient_max_value, gradient_max_value), var) for grad, var in gvs]
        train_op = optimizer.apply_gradients(capped_gvs)

        # декодируем выход модели (decoded) и сравниваем истинные метки и получившиеся по расстоянию
        # Левенштейна между последовательностями
        # decoded, log_prob = tf.nn.ctc_beam_search_decoder(logits, seq_len)
        decoded, log_prob = tf.nn.ctc_greedy_decoder(logits, seq_len)
        ler = tf.reduce_mean(tf.edit_distance(tf.cast(decoded[0], tf.int32), targets))

    # Отбираем образцы, у которых bottom_len_limit <= символов <= top_len_limit, и выбираем батчи из них
    X_train_limit, y_train_limit = sample_gen.get_X_y_by_limits(bottom_len_limit, top_len_limit)
    num_examples = len(X_train_limit)

    # writer = tf.summary.FileWriter("logs", graph=graph)

    # Загружаем веса из предварительно обученной двухслойной модели
    load_2layer_weights = {}
    for filename in os.listdir(weights_dir_2layer):
        loaded_array = np.load(weights_dir_2layer + '/' + filename)
        variable_name = filename.replace(".", "/").replace("/npy", "")
        load_2layer_weights[variable_name] = loaded_array

    with tf.Session(graph=graph, config=config) as session:
        saver = tf.train.Saver(tf.global_variables())
        checkpoint = tf.train.latest_checkpoint(checkpoint_dir=checkpoint_dir_3layer)
        last_epoch = 0

        if checkpoint is not None:
            print('Loading checkpoint {}'.format(checkpoint))
            try:
                saver.restore(session, checkpoint)
                last_epoch = int(checkpoint.split('-')[-1]) + 1
                print('Start from epoch {}'.format(last_epoch))
            except ValueError:
                print('Incompatible checkpoint, restarting from 0')
        else:
            tf.global_variables_initializer().run()
            # Устанавливаем значения переменных из загруженных весов 2-х слойной модели
            # Слои: 1 -> 1, 2 -> 2,3, dense -> dense
            for var_name, var_np_value in load_2layer_weights.items():
                var_tf_value = tf.convert_to_tensor(var_np_value, np.float32)
                variable = tf.get_default_graph().get_tensor_by_name(var_name)
                session.run(tf.assign(variable, var_tf_value))
            #     # Копируем коэффициенты из 2-го слоя в 3-ий (новый) слой
            #     if "bidirectional_2" in var_name:
            #         var_name_layer3 = var_name.replace("_2", "_3")
            #         variable = tf.get_default_graph().get_tensor_by_name(var_name_layer3)
            #         session.run(tf.assign(variable, var_tf_value))

        # Начинаем обучение с этими весами
        for curr_epoch in range(last_epoch, last_epoch + num_epochs + 1):
            logger.info("======   Start epoch {}   ======".format(curr_epoch))

            if curr_epoch % epoch_save_step == 0 and curr_epoch > 0:
                print('Saving snapshot {} on epoch {}'.format(snapshot_file_name, curr_epoch))
                saver.save(session, checkpoint_dir_3layer + '/' + snapshot_file_name + ".ckpt", curr_epoch)

            train_cost = train_ler = 0

            batch_num = 1
            for train_inputs, seq_lengths_batch, train_targets, y_batch_origin in \
                    sample_gen.get_batch(X_train_limit, y_train_limit, batch_size):
                feed = {inputs: train_inputs,
                        targets: train_targets,
                        seq_len: seq_lengths_batch}

                train_logits, batch_cost, _ = session.run([logits, cost, train_op], feed)
                train_cost += batch_cost * batch_size
                train_ler += session.run(ler, feed_dict=feed) * batch_size

                if batch_num % 20 == 0:
                    print('Batch number {} done. Max seq length: {}; train_cost: {}, train_ler: {}'
                          .format(batch_num, max(seq_lengths_batch), train_cost / (batch_size * batch_num),
                                  train_ler / (batch_size * batch_num)))

                batch_num += 1

            train_cost /= num_examples
            train_ler /= num_examples
            print("Epoch: {}, train_cost: {}, train_ler: {}".format(curr_epoch, train_cost, train_ler))

            # После окончания эпохи декодируем результат
            d = session.run(decoded[0], feed_dict=feed)
            str_decoded_list = decode_sparse(d)
            original_list = decode_indexes_to_string(y_batch_origin)
            for index, str_decoded in enumerate(str_decoded_list):
                # Выводим только непустые результаты
                if len(str_decoded) > 1 or (len(str_decoded) == 1 and str_decoded[0] != ' '):
                    print('Original[{}]: {}'.format(index, original_list[index]))
                    print('Decoded[{}] : {}'.format(index, str_decoded))
