import tensorflow as tf 
import tensorflow_probability as tfp 

from keras.saving import register_keras_serializable

tfd, tfb = tfp.distributions, tfp.bijectors
tf.keras.utils.set_random_seed(31)
bce = tf.keras.losses.BinaryCrossentropy()

def loss_function(y_true, alpha_beta):
    y = tf.reshape(y_true, (-1,1))
    alpha, beta = tf.split(alpha_beta, 2, axis=-1)
    p_hat = alpha / (alpha + beta)
    return bce(y, p_hat)

# Metric for Binary Accuracy on Mean
class MeanBinaryAccuracy(tf.keras.metrics.Metric):

    def __init__(self, name="mean_binary_accuracy", **kw):
        super().__init__(name=name, **kw)
        self.ba = tf.keras.metrics.BinaryAccuracy()

    def update_state(self, y_true, alpha_beta, sample_weight=None):
        alpha, beta = tf.split(alpha_beta, 2, axis=-1)
        mean = tf.squeeze(alpha / (alpha + beta), axis=-1)
        self.ba.update_state(y_true, mean, sample_weight)

    def result(self):
        return self.ba.result()

    def reset_states(self):
        self.ba.reset_states()


class MeanAUC(tf.keras.metrics.AUC):

    def __init__(self, name="auc", **kw):
        super().__init__(name=name, from_logits=False, **kw)

    def update_state(self, y_true, y_pred_ab, sample_weight=None):
        alpha, beta = tf.split(y_pred_ab, 2, axis=-1)
        probs = tf.squeeze(alpha / (alpha + beta), axis=-1)  
        return super().update_state(y_true, probs, sample_weight)

@register_keras_serializable(package="color")
class ColorModel(tf.keras.Model):

    def __init__(self, hidden_dims=(32,64), hue_embed_dim=8, **kw):
        super().__init__(**kw)
        
        # 2D CNN 
        self.backbone = tf.keras.Sequential([
            tf.keras.layers.Conv2D(hidden_dims[0], 3, activation='relu'),
            tf.keras.layers.MaxPool2D(),
            tf.keras.layers.Conv2D(hidden_dims[1], 3, activation='relu'),
            tf.keras.layers.GlobalAveragePooling2D()
        ])

        self.hue_embed = tf.keras.Sequential([
            tf.keras.layers.Dense(hue_embed_dim*4, activation='relu'),
            tf.keras.layers.Dense(hue_embed_dim*2, activation='relu'),
            tf.keras.layers.Dense(hue_embed_dim, activation='relu'),
        ])

        self.alpha_beta = tf.keras.layers.Dense(2, activation=tf.nn.softplus, bias_initializer='zeros')

    def call(self, inputs, training=False):
        img = inputs["img"]
        hue = inputs["hue"]

        f_img = self.backbone(img, training=training)
        f_hue = self.hue_embed(hue)

        feats = tf.concat([f_img, f_hue], axis=-1)
        return self.alpha_beta(feats)
