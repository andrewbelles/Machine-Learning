import tensorflow as tf 

AUTOTUNE = tf.data.AUTOTUNE
target_size = (64, 64)


def load_image(path: str) -> tf.Tensor:
    img_bytes = tf.io.read_file(path)
    img = tf.io.decode_image(img_bytes, channels=3)

    img.set_shape([None, None, 3])
    img = tf.image.resize(img, target_size)
    return tf.cast(img, tf.float32) / 255.0


def create_dataset(
        csv_path: str, 
        image_dir: str="images", 
        batch: int=64,
        validation_split: float = 0.2,
        shuffle: bool=True,
        seed: int = 31):    

    # Expect filename,hue,label
    column_types = [tf.string, tf.float32, tf.int32]
    dataset = tf.data.experimental.CsvDataset(
        filenames=csv_path, 
        record_defaults=column_types,
        header=True 
    )

    # Set path from image directory 
    def _add_path(filename, hue, label):
        full = tf.strings.join([image_dir, "/", filename])
        return full, hue, tf.cast(label, tf.float32)
    dataset = dataset.map(_add_path, num_parallel_calls=AUTOTUNE)

    # Get images for each filename 
    def _load(full_path, hue, label):
        patch = load_image(full_path)
        hue_vec = tf.expand_dims(hue, 0)
        return {"img": patch, "hue": hue_vec}, label
    dataset = dataset.map(_load, num_parallel_calls=AUTOTUNE)

    n = tf.data.experimental.cardinality(dataset).numpy()
    validation_count = int(validation_split * n)

    if shuffle:
        dataset = dataset.shuffle(n, seed=seed, reshuffle_each_iteration=True)

    vds = dataset.take(validation_count)
    tds   = dataset.skip(validation_count)

    validation = vds.batch(batch).prefetch(4)
    training   = tds.batch(batch).prefetch(4)

    return training, validation 

