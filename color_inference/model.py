import tensorflow as tf 
import tensorflow_probability as tfp

from keras.saving import register_keras_serializable

tfd, tfb = tfp.distributions, tfp.bijectors
tf.keras.utils.set_random_seed(31)
bce = tf.keras.losses.BinaryCrossentropy()
TOL  = 1e-3
CONC = 20.0

def bce_loss(y_true, y_pred):
    y_true = tf.reshape(y_true, (-1,1))
    return bce(y_true, y_pred)

def beta_loss(y_true, alpha_beta):
    y = tf.reshape(y_true, (-1,1))
    y = tf.clip_by_value(y, 1e-4, 1.0 - 1e-4)

    alpha, beta = tf.split(alpha_beta, 2, axis=-1)
    alpha = tf.clip_by_value(alpha + 1.0, TOL, CONC)
    beta  = tf.clip_by_value(beta + 1.0, TOL, CONC)

    # Get clipped beta distribution and compute negative log likelihood 
    beta_dist = tfd.Beta(alpha, beta)
    nll = -tf.reduce_mean(beta_dist.log_prob(y))

    kl = tf.reduce_mean(tfd.kl_divergence(beta_dist,
                                         tfd.Beta(concentration1=1.0,
                                                  concentration0=1.0)))
    return nll + TOL * kl

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

    def __init__(self, head_type='beta', hidden_dims=(32,64,128), hue_embed_dim=(8,16,32), **kw):
        super().__init__(**kw)
        self.head_type     = head_type
        self.hidden_dims   = tuple(hidden_dims)
        self.hue_embed_dim = tuple(hue_embed_dim)

        # 3 Layer convolutional backbone  
        self.backbone = tf.keras.Sequential([
            tf.keras.layers.Conv2D(hidden_dims[0], 3, activation='relu'),
            tf.keras.layers.MaxPool2D(),
            tf.keras.layers.Conv2D(hidden_dims[1], 3, activation='relu'),
            tf.keras.layers.MaxPool2D(),
            tf.keras.layers.Conv2D(hidden_dims[2], 3, activation='relu'),
            tf.keras.layers.GlobalAveragePooling2D()
        ])

        self.hue_embed = tf.keras.Sequential([
            tf.keras.layers.Dense(hue_embed_dim[2], activation='relu'),
            tf.keras.layers.Dense(hue_embed_dim[1], activation='relu'),
            tf.keras.layers.Dense(hue_embed_dim[0], activation='relu'),
        ])

        if self.head_type == "sigmoid":
            self.head = tf.keras.layers.Dense(1, activation="sigmoid")
        elif self.head_type == "beta":
            # beta head: two conc. params, clipped
            self.head = tf.keras.layers.Dense(2, activation=tf.nn.softplus)

        self.clip = CONC 

    def get_config(self):
            config = super().get_config()
            config.update({
                'head_type': self.head_type,
                'hidden_dims': list(self.hidden_dims),
                'hue_embed_dim': list(self.hue_embed_dim),
            })
            return config

    @classmethod
    def from_config(cls, config):
        return cls(**config)
    
    def call(self, inputs, training=False):
        img = inputs["img"]
        hue = inputs["hue"]

        f_img = self.backbone(img, training=training)
        f_hue = self.hue_embed(hue)

        feats = tf.concat([f_img, f_hue], axis=-1)

        out = self.head(feats)   # one call
        if self.head_type == "beta":
            out = tf.clip_by_value(out + 1.0, 1.0, self.clip)
        return out

    def set_head(self, new_head):
        self.head = new_head
