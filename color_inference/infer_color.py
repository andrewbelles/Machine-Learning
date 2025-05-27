import argparse as ap, tensorflow as tf 
import tensorflow_probability as tfp
from image_pipeline import load_image
import model as cm

def main():
    # Path to image argument  
    parser = ap.ArgumentParser()
    parser.add_argument("--hue", type=float, required=True, help="Input hue to infer [0,360]")
    parser.add_argument("--image", required=True, help="Path to PNG")
    parser.add_argument("--model", required=False, default="beta_color_detector", help="Path to Model")
    args = parser.parse_args()

    path_to_model = "models/" + args.model + "_color_detector.keras"
    model = tf.keras.models.load_model(path_to_model, compile=False, custom_objects={'ColorModel': cm.ColorModel})

    patch = load_image(args.image)[None, ...]
    hue = tf.constant([[float(args.hue/360.0)]], tf.float32)

    out = model({"img": patch, "hue": hue}, training=False)

    # format output based on head
    if out.shape[-1] == 1:
        # sigmoid head
        print(f"P(hue={args.hue:.1f}) = {out.numpy().item():.2%}")
    else:
        # beta head
        alpha, beta = tf.split(out, 2, axis=-1)
        mean = (alpha/(alpha+beta)).numpy().item()
        q05, q95 = tfp.distributions.Beta(alpha, beta).quantile([.05,.95]).numpy().flatten()
        print(f"P(hue={args.hue:.1f}) = {mean:.2%} (90% CI [{q05:.2%}, {q95:.2%}])")

if __name__ == "__main__":
    main()
