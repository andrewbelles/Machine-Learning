import argparse as ap, tensorflow as tf 
import tensorflow_probability as tfp
from image_pipeline import load_image
import model

def main():
    # Path to image argument  
    parser = ap.ArgumentParser()
    parser.add_argument("--hue", type=float, required=True, help="Input hue to infer [0,360]")
    parser.add_argument("--image", required=True, help="Path to PNG")
    args = parser.parse_args()

    model = tf.keras.models.load_model("models/beta_color_detector.keras", compile=False)

    patch = load_image(args.image)[None, ...]
    hue = tf.constant([[float(args.hue/360.0)]], tf.float32)

    alpha_beta = model({"img": patch, "hue": hue}, training=False)
    alpha, beta = tf.split(alpha_beta, 2, axis=-1)

    mean = (alpha / (alpha + beta)).numpy().item()
    q05, q95 = tfp.distributions.Beta(alpha, beta).quantile([0.05, 0.95]).numpy().flatten()

    print(f"P(hue={args.hue:.3f} present) = {mean:.2%} (90% CI: [{q05:.2%}, {q95:.2%}])")

if __name__ == "__main__":
    main()
