
# Восстанавливаем модель из snapshot'а и сохраняем веса слоёв
def save_network_weights():
    hidden_units = 128

    graph = tf.Graph()
    with graph.as_default():
        tf.add_check_numerics_ops()

        model = Sequential()
        model.add(
            Bidirectional(CuDNNLSTM(units=hidden_units, return_sequences=True),
                          input_shape=(None, num_features)))
        model.add(
            Bidirectional(CuDNNLSTM(units=hidden_units, return_sequences=True)))
        model.add(TimeDistributed(Dense(num_classes)))
        model.summary()

    with tf.Session(graph=graph) as session:
        saver = tf.train.Saver(tf.global_variables())
        checkpoint = tf.train.latest_checkpoint(checkpoint_dir=checkpoint_dir_2layer)
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

        list_trainable_variables = tf.trainable_variables()
        for trainable_variable in list_trainable_variables:
            variable_value = session.run(trainable_variable)
            variable_name = trainable_variable.name.replace("/", ".")
            np.save(weights_dir_2layer + "/" + variable_name, variable_value)
