import os

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["ABSL_MIN_LOG_LEVEL"] = "3"
import argparse as ap

import tensorflow as tf

import image_pipeline as ppl
import model as cm

def get_model(new: bool, head_type: str, model_path: str) -> tf.keras.Model:

    if new:
        model = cm.ColorModel(head_type=head_type)
    else:
        model = tf.keras.models.load_model(model_path, compile=False, custom_objects={'ColorModel': cm.ColorModel})
    
    if head_type == "sigmoid":
        loss = cm.bce_loss            
        metrics   = [tf.keras.metrics.BinaryAccuracy(name="acc")]
    else:
        loss = cm.beta_loss           
        metrics   = [cm.MeanBinaryAccuracy(), cm.MeanAUC()]

    model.compile(optimizer="adam", loss=loss, metrics=metrics)
    return model

def main() -> None:
    # Standard arguments for training params 
    parser = ap.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=50, help="# of training epochs")
    parser.add_argument("--new", type=bool, default=False, help="Indicate whether a new model should be trained")
    parser.add_argument("--model", default="beta", help="Indicate head of model to train")
    parser.add_argument("--early", default=False, help="Denote early stop callback being active")
    args = parser.parse_args()

    model_path = "models/" + args.model + "_color_detector.keras"

    # Fetch batched datasets  
    training, validation = ppl.create_dataset(
            csv_path="images/labels.csv", 
            image_dir="images", 
            batch=64,
            validation_split=0.2)

    # Get model
    model = get_model(args.new, args.model, model_path)

    early_cb = tf.keras.callbacks.EarlyStopping(
        monitor='val_loss',
        min_delta=1e-2,
        patience=3,
        mode='auto',
        restore_best_weights=True
    )

    # Callback for tensorboard
    tensorboard_cb = tf.keras.callbacks.TensorBoard(
        log_dir="logs/fit",
        histogram_freq=1,
    )

    callbacks = [tensorboard_cb] + ([early_cb] if args.early else [])

    model.fit(training, validation_data=validation, epochs=args.epochs, callbacks=callbacks)
    model.save(model_path)  # Save model 

if __name__ == "__main__":
    main()
