import os 
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["ABSL_MIN_LOG_LEVEL"] = "3"
import argparse as ap

import tensorflow as tf

import image_pipeline as ppl
import model as cm

def get_model(new: bool) -> tf.keras.Model:
    if new:
        model = cm.ColorModel()
    else:
        model = tf.keras.models.load_model(
            "models/beta_color_detector.keras",
            compile=False)          # already compiled previously

    model.compile(optimizer="adam",
                loss=cm.loss_function,
                metrics=[cm.MeanBinaryAccuracy(), cm.MeanAUC()])

    return model

def main() -> None:
    # Standard arguments for training params 
    parser = ap.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=50, help="# of training epochs")
    parser.add_argument("--new", type=bool, default=False, help="Indicate whether a new model should be trained")
    args = parser.parse_args()

    # Fetch batched datasets  
    training, validation = ppl.create_dataset(
            csv_path="images/labels.csv", 
            image_dir="images", 
            batch=64,
            validation_split=0.2)

    # Create model
    model = get_model(args.new)

    # Callback for tensorboard
    tensorboard_cb = tf.keras.callbacks.TensorBoard(
        log_dir="logs/fit",
        histogram_freq=1,
    )

    model.fit(training, validation_data=validation, epochs=args.epochs, callbacks=[tensorboard_cb])
    model.save("models/beta_color_detector.keras")  # Save model 

if __name__ == "__main__":
    main()
