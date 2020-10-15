import errno
import glob
import gzip
import json
import os
import pickle
import random
import shutil
import sys
import tarfile
import zipfile

import PIL
import six
from six.moves.urllib.error import HTTPError, URLError

stderr = sys.stderr
sys.stderr = open(os.devnull, 'w')
import keras

sys.stderr = stderr
import keras.backend as K
import numpy as np
import tensorflow as tf
from PIL import Image, ExifTags
from keras.layers import Dense, Activation, Dropout
from keras.models import Model
from keras.preprocessing import image
from skimage.transform import resize

from fawkes.align_face import align
from six.moves.urllib.request import urlopen
import time
import datetime

if sys.version_info[0] == 2:
    def urlretrieve(url, filename, reporthook=None, data=None):
        def chunk_read(response, chunk_size=8192, reporthook=None):
            content_type = response.info().get('Content-Length')
            total_size = -1
            if content_type is not None:
                total_size = int(content_type.strip())
            count = 0
            while True:
                chunk = response.read(chunk_size)
                count += 1
                if reporthook is not None:
                    reporthook(count, chunk_size, total_size)
                if chunk:
                    yield chunk
                else:
                    break

        response = urlopen(url, data)
        with open(filename, 'wb') as fd:
            for chunk in chunk_read(response, reporthook=reporthook):
                fd.write(chunk)
else:
    from six.moves.urllib.request import urlretrieve


def clip_img(X, preprocessing='raw'):
    X = reverse_preprocess(X, preprocessing)
    X = np.clip(X, 0.0, 255.0)
    X = preprocess(X, preprocessing)
    return X


def load_image(path):
    try:
        img = Image.open(path)
    except PIL.UnidentifiedImageError:
        return None
    except IsADirectoryError:
        return None

    if img._getexif() is not None:
        for orientation in ExifTags.TAGS.keys():
            if ExifTags.TAGS[orientation] == 'Orientation':
                break

        exif = dict(img._getexif().items())
        if orientation in exif.keys():
            if exif[orientation] == 3:
                img = img.rotate(180, expand=True)
            elif exif[orientation] == 6:
                img = img.rotate(270, expand=True)
            elif exif[orientation] == 8:
                img = img.rotate(90, expand=True)
            else:
                pass
    img = img.convert('RGB')
    image_array = image.img_to_array(img)

    return image_array


def filter_image_paths(image_paths):
    print("Identify {} files in the directory".format(len(image_paths)))
    new_image_paths = []
    new_images = []
    for p in image_paths:
        img = load_image(p)
        if img is None:
            print("{} is not an image file, skipped".format(p.split("/")[-1]))
            continue
        new_image_paths.append(p)
        new_images.append(img)
    print("Identify {} images in the directory".format(len(new_image_paths)))
    return new_image_paths, new_images


class Faces(object):
    def __init__(self, image_paths, loaded_images, aligner, verbose=1, eval_local=False):
        self.image_paths = image_paths
        self.verbose = verbose
        self.aligner = aligner
        self.org_faces = []
        self.cropped_faces = []
        self.cropped_faces_shape = []
        self.cropped_index = []
        self.callback_idx = []
        for i in range(0, len(loaded_images)):
            cur_img = loaded_images[i]
            p = image_paths[i]

            self.org_faces.append(cur_img)

            if eval_local:
                margin = 0
            else:
                margin = 0.7
            align_img = align(cur_img, self.aligner, margin=margin)

            if align_img is None:
                print("Find 0 face(s)".format(p.split("/")[-1]))
                continue

            cur_faces = align_img[0]

            cur_shapes = [f.shape[:-1] for f in cur_faces]

            cur_faces_square = []
            if verbose:
                print("Find {} face(s) in {}".format(len(cur_faces), p.split("/")[-1]))

            if eval_local:
                cur_faces = cur_faces[:1]

            for img in cur_faces:
                if eval_local:
                    base = resize(img, (224, 224))
                else:
                    long_size = max([img.shape[1], img.shape[0]])
                    base = np.zeros((long_size, long_size, 3))
                    base[0:img.shape[0], 0:img.shape[1], :] = img
                cur_faces_square.append(base)

            cur_index = align_img[1]
            cur_faces_square = [resize(f, (224, 224)) for f in cur_faces_square]
            self.cropped_faces_shape.extend(cur_shapes)
            self.cropped_faces.extend(cur_faces_square)
            self.cropped_index.extend(cur_index)
            self.callback_idx.extend([i] * len(cur_faces_square))

        if not self.cropped_faces:
            raise FaceNotFoundError("No faces detected")

        self.cropped_faces = np.array(self.cropped_faces)

        self.cropped_faces = preprocess(self.cropped_faces, 'imagenet')

        self.cloaked_cropped_faces = None
        self.cloaked_faces = np.copy(self.org_faces)

    def get_faces(self):
        return self.cropped_faces

    def merge_faces(self, cloaks):

        self.cloaked_faces = np.copy(self.org_faces)

        for i in range(len(self.cropped_faces)):
            cur_cloak = cloaks[i]
            org_shape = self.cropped_faces_shape[i]
            old_square_shape = max([org_shape[0], org_shape[1]])
            reshape_cloak = resize(cur_cloak, (old_square_shape, old_square_shape))
            reshape_cloak = reshape_cloak[0:org_shape[0], 0:org_shape[1], :]

            callback_id = self.callback_idx[i]
            bb = self.cropped_index[i]
            self.cloaked_faces[callback_id][bb[1]:bb[3], bb[0]:bb[2], :] += reshape_cloak

        return self.cloaked_faces


def dump_dictionary_as_json(dict, outfile):
    j = json.dumps(dict)
    with open(outfile, "wb") as f:
        f.write(j.encode())


def load_victim_model(number_classes, teacher_model=None, end2end=False, dropout=0):
    for l in teacher_model.layers:
        l.trainable = end2end
    x = teacher_model.layers[-1].output
    if dropout > 0:
        x = Dropout(dropout)(x)
    x = Dense(number_classes)(x)
    x = Activation('softmax', name="act")(x)
    model = Model(teacher_model.input, x)
    opt = keras.optimizers.Adadelta()
    model.compile(loss='categorical_crossentropy', optimizer=opt, metrics=['accuracy'])
    return model


def init_gpu(gpu_index, force=False):
    if isinstance(gpu_index, list):
        gpu_num = ','.join([str(i) for i in gpu_index])
    else:
        gpu_num = str(gpu_index)
    #if "CUDA_VISIBLE_DEVICES" in os.environ and os.environ["CUDA_VISIBLE_DEVICES"] and not force:
    #    print('GPU already initiated')
    #    return
    os.environ["CUDA_VISIBLE_DEVICES"] = gpu_num
    sess = fix_gpu_memory()
    return sess


def fix_gpu_memory(mem_fraction=1):
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
    tf_config = None
    if tf.test.is_gpu_available():
        gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction=mem_fraction)
        tf_config = tf.ConfigProto(gpu_options=gpu_options)
        tf_config.gpu_options.allow_growth = True
        tf_config.log_device_placement = False
    init_op = tf.global_variables_initializer()
    sess = tf.Session(config=tf_config)
    sess.run(init_op)
    K.set_session(sess)
    return sess


def preprocess(X, method):
    assert method in {'raw', 'imagenet', 'inception', 'mnist'}

    if method == 'raw':
        pass
    elif method == 'imagenet':
        X = imagenet_preprocessing(X)
    else:
        raise Exception('unknown method %s' % method)

    return X


def reverse_preprocess(X, method):
    assert method in {'raw', 'imagenet', 'inception', 'mnist'}

    if method == 'raw':
        pass
    elif method == 'imagenet':
        X = imagenet_reverse_preprocessing(X)
    else:
        raise Exception('unknown method %s' % method)

    return X


def imagenet_preprocessing(x, data_format=None):
    if data_format is None:
        data_format = K.image_data_format()
    assert data_format in ('channels_last', 'channels_first')

    x = np.array(x)
    if data_format == 'channels_first':
        # 'RGB'->'BGR'
        if x.ndim == 3:
            x = x[::-1, ...]
        else:
            x = x[:, ::-1, ...]
    else:
        # 'RGB'->'BGR'
        x = x[..., ::-1]

    mean = [103.939, 116.779, 123.68]
    std = None

    # Zero-center by mean pixel
    if data_format == 'channels_first':
        if x.ndim == 3:
            x[0, :, :] -= mean[0]
            x[1, :, :] -= mean[1]
            x[2, :, :] -= mean[2]
            if std is not None:
                x[0, :, :] /= std[0]
                x[1, :, :] /= std[1]
                x[2, :, :] /= std[2]
        else:
            x[:, 0, :, :] -= mean[0]
            x[:, 1, :, :] -= mean[1]
            x[:, 2, :, :] -= mean[2]
            if std is not None:
                x[:, 0, :, :] /= std[0]
                x[:, 1, :, :] /= std[1]
                x[:, 2, :, :] /= std[2]
    else:
        x[..., 0] -= mean[0]
        x[..., 1] -= mean[1]
        x[..., 2] -= mean[2]
        if std is not None:
            x[..., 0] /= std[0]
            x[..., 1] /= std[1]
            x[..., 2] /= std[2]

    return x


def imagenet_reverse_preprocessing(x, data_format=None):
    import keras.backend as K
    x = np.array(x)
    if data_format is None:
        data_format = K.image_data_format()
    assert data_format in ('channels_last', 'channels_first')

    if data_format == 'channels_first':
        if x.ndim == 3:
            # Zero-center by mean pixel
            x[0, :, :] += 103.939
            x[1, :, :] += 116.779
            x[2, :, :] += 123.68
            # 'BGR'->'RGB'
            x = x[::-1, :, :]
        else:
            x[:, 0, :, :] += 103.939
            x[:, 1, :, :] += 116.779
            x[:, 2, :, :] += 123.68
            x = x[:, ::-1, :, :]
    else:
        # Zero-center by mean pixel
        x[..., 0] += 103.939
        x[..., 1] += 116.779
        x[..., 2] += 123.68
        # 'BGR'->'RGB'
        x = x[..., ::-1]
    return x


def reverse_process_cloaked(x, preprocess='imagenet'):
    # x = clip_img(x, preprocess)
    return reverse_preprocess(x, preprocess)


def build_bottleneck_model(model, cut_off):
    bottleneck_model = Model(model.input, model.get_layer(cut_off).output)
    bottleneck_model.compile(loss='categorical_crossentropy',
                             optimizer='adam',
                             metrics=['accuracy'])
    return bottleneck_model


def load_extractor(name):
    model_dir = os.path.join(os.path.expanduser('~'), '.fawkes')
    os.makedirs(model_dir, exist_ok=True)
    model_file = os.path.join(model_dir, "{}.h5".format(name))
    emb_file = os.path.join(model_dir, "{}_emb.p.gz".format(name))
    #if os.path.exists(model_file):
    model = keras.models.load_model(model_file)
    #else:
    #    print("Download models...")
    #    get_file("{}.h5".format(name), "http://sandlab.cs.uchicago.edu/fawkes/files/{}.h5".format(name),
    #             cache_dir=model_dir, cache_subdir='')
    #    model = keras.models.load_model(model_file)

    #if not os.path.exists(emb_file):
    #    get_file("{}_emb.p.gz".format(name), "http://sandlab.cs.uchicago.edu/fawkes/files/{}_emb.p.gz".format(name),
    #             cache_dir=model_dir, cache_subdir='')

    if hasattr(model.layers[-1], "activation") and model.layers[-1].activation == "softmax":
        raise Exception(
            "Given extractor's last layer is softmax, need to remove the top layers to make it into a feature extractor")
    return model


def get_dataset_path(dataset):
    model_dir = os.path.join(os.path.expanduser('~'), '.fawkes')
    if not os.path.exists(os.path.join(model_dir, "config.json")):
        raise Exception("Please config the datasets before running protection code. See more in README and config.py.")

    config = json.load(open(os.path.join(model_dir, "config.json"), 'r'))
    if dataset not in config:
        raise Exception(
            "Dataset {} does not exist, please download to data/ and add the path to this function... Abort".format(
                dataset))
    return config[dataset]['train_dir'], config[dataset]['test_dir'], config[dataset]['num_classes'], config[dataset][
        'num_images']


def normalize(x):
    return x / np.linalg.norm(x, axis=1, keepdims=True)


def dump_image(x, filename, format="png", scale=False):
    # img = image.array_to_img(x, scale=scale)
    img = image.array_to_img(x)
    filename = filename[:-4] + '.jpg'
    img.save(filename, 'jpeg')
    return


def load_dir(path):
    assert os.path.exists(path)
    x_ls = []
    for file in os.listdir(path):
        cur_path = os.path.join(path, file)
        im = image.load_img(cur_path, target_size=(224, 224))
        im = image.img_to_array(im)
        x_ls.append(im)
    raw_x = np.array(x_ls)
    return preprocess(raw_x, 'imagenet')


def load_embeddings(feature_extractors_names):
    model_dir = os.path.join(os.path.expanduser('~'), '.fawkes')
    dictionaries = []
    for extractor_name in feature_extractors_names:
        fp = gzip.open(os.path.join(model_dir, "{}_emb.p.gz".format(extractor_name)), 'rb')
        path2emb = pickle.load(fp)
        fp.close()
        dictionaries.append(path2emb)

    merge_dict = {}
    for k in dictionaries[0].keys():
        cur_emb = [dic[k] for dic in dictionaries]
        merge_dict[k] = np.concatenate(cur_emb)
    return merge_dict


def extractor_ls_predict(feature_extractors_ls, X):
    feature_ls = []
    for extractor in feature_extractors_ls:
        cur_features = extractor.predict(X)
        feature_ls.append(cur_features)
    concated_feature_ls = np.concatenate(feature_ls, axis=1)
    concated_feature_ls = normalize(concated_feature_ls)
    return concated_feature_ls


def pairwise_l2_distance(A, B):
    BT = B.transpose()
    vecProd = np.dot(A, BT)
    SqA = A ** 2
    sumSqA = np.matrix(np.sum(SqA, axis=1))
    sumSqAEx = np.tile(sumSqA.transpose(), (1, vecProd.shape[1]))

    SqB = B ** 2
    sumSqB = np.sum(SqB, axis=1)
    sumSqBEx = np.tile(sumSqB, (vecProd.shape[0], 1))
    SqED = sumSqBEx + sumSqAEx - 2 * vecProd
    SqED[SqED < 0] = 0.0
    ED = np.sqrt(SqED)
    return ED


def calculate_dist_score(a, b, feature_extractors_ls, metric='l2'):
    features1 = extractor_ls_predict(feature_extractors_ls, a)
    features2 = extractor_ls_predict(feature_extractors_ls, b)

    pair_cos = pairwise_l2_distance(features1, features2)
    max_sum = np.min(pair_cos, axis=0)
    max_sum_arg = np.argsort(max_sum)[::-1]
    max_sum_arg = max_sum_arg[:len(a)]
    max_sum = [max_sum[i] for i in max_sum_arg]
    paired_target_X = [b[j] for j in max_sum_arg]
    paired_target_X = np.array(paired_target_X)
    return np.min(max_sum), paired_target_X


def select_target_label(imgs, feature_extractors_ls, feature_extractors_names, metric='l2'):
    model_dir = os.path.join(os.path.expanduser('~'), '.fawkes')
    print('dog', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), flush=True)
    original_feature_x = extractor_ls_predict(feature_extractors_ls, imgs)
    print('cat', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), flush=True)
    path2emb = load_embeddings(feature_extractors_names)
    exclude_list = [1691, 19236, 20552, 9231, 18221, 8250, 18785, 6989, 17170,
                    1704, 19394, 6058, 3327, 11885, 20375, 19150, 676, 11663,
                    17261, 3527, 3956, 1973, 1197, 4859, 590, 13873, 928,
                    14397, 4288, 3393, 6975, 16988, 1269, 323, 6409, 588,
                    19738, 1845, 12123, 2714, 5318, 15325, 19268, 4650, 4714,
                    3953, 6715, 6015, 12668, 13933, 14306, 2768, 20597, 4578,
                    1278, 17549, 19355, 8882, 3276, 9148, 14517, 14915, 18209,
                    3162, 8615, 18647, 749, 19259, 11490, 16046, 13259, 4429,
                    10705, 12258, 13699, 4323, 15112, 14170, 3520, 17180, 5195,
                    728, 2680, 13117, 20241, 15320, 8079, 2894, 11533, 10083,
                    9628, 14944, 13124, 13316, 8006, 15353, 15261, 8865, 1213,
                    1469, 20777, 9868, 10972, 9058, 18890, 13178, 13772, 15675,
                    10572, 8771, 14211, 18781, 16347, 17985, 11456, 5849, 15709,
                    20856, 2590, 15964, 8377, 5465, 16928, 13063, 19766, 19643,
                    8651, 8517, 5985, 14817, 18926, 3791, 1864, 20061, 7697,
                    13449, 19525, 13131, 421, 7629, 14689, 17521, 4509, 19374,
                    17584, 11055, 11929, 17117, 7492, 14182, 409, 14294, 15033,
                    10074, 9081, 7682, 19306, 3674, 945, 13211, 10933, 17953,
                    12729, 8087, 20723, 5396, 14015, 20110, 15186, 6939, 239,
                    2393, 17326, 13712, 9921, 7997, 6215, 14582, 864, 18906,
                    9351, 9178, 3600, 18567, 8614, 19429, 286, 10042, 13030,
                    7076, 3370, 15285, 7925, 10851, 5155, 14732, 12051, 11334,
                    17035, 15476]
    exclude_list = set(exclude_list)

    items = list([(k, v) for k, v in path2emb.items() if k not in exclude_list])
    paths = [p[0] for p in items]
    embs = [p[1] for p in items]
    embs = np.array(embs)
    #print('rabbit', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), flush=True)
    #np.set_printoptions(threshold=sys.maxsize)
    pair_dist = pairwise_l2_distance(original_feature_x, embs)
    pair_dist = np.array(pair_dist)
    pair_dist_std = np.sort(pair_dist[0],axis=0)
    #print('hog', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), flush=True)
    max_sum = np.min(pair_dist, axis=0)
    max_id_ls = np.argsort(max_sum)[::-1]
    random.seed(datetime.datetime.now())
    max_id = random.choice(max_id_ls[10:30])

    #print(pair_dist.shape, flush=True)

    #print(max_sum.shape, flush=True)
    #print(pair_dist_std[-200:-1], flush=True)
    #print(max_sum[max_id],flush=True)

    target_data_id = paths[int(max_id)]

    image_dir = os.path.join(model_dir, "target_data/{}".format(target_data_id))
    #print(np.sort(pair_dist, axis=0), flush=True)
    # if not os.path.exists(image_dir):

    #print('frog', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), flush=True)
    #os.makedirs(os.path.join(model_dir, "target_data"), exist_ok=True)
    #os.makedirs(image_dir, exist_ok=True)
    #for i in range(10):
    #    if os.path.exists(os.path.join(model_dir, "target_data/{}/{}.jpg".format(target_data_id, i))):
    #        continue
    #    try:
    #        get_file("{}.jpg".format(i),
    #                 "http://sandlab.cs.uchicago.edu/fawkes/files/target_data/{}/{}.jpg".format(target_data_id, i),
    #                 cache_dir=model_dir, cache_subdir='target_data/{}/'.format(target_data_id))
    #    except Exception:
    #        pass
    #print('toad', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), flush=True)

    image_paths = glob.glob(image_dir + "/*.jpg")

    target_images = [image.img_to_array(image.load_img(cur_path)) for cur_path in
                     image_paths]

    target_images = np.array([resize(x, (224, 224)) for x in target_images])
    target_images = preprocess(target_images, 'imagenet')

    target_images = list(target_images)
    while len(target_images) < len(imgs):
        target_images += target_images

    target_images = random.sample(target_images, len(imgs))
    print('iguana', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), flush=True)
    return np.array(target_images)


def get_file(fname,
             origin,
             untar=False,
             md5_hash=None,
             file_hash=None,
             cache_subdir='datasets',
             hash_algorithm='auto',
             extract=False,
             archive_format='auto',
             cache_dir=None):
    if cache_dir is None:
        cache_dir = os.path.join(os.path.expanduser('~'), '.fawkes')
    if md5_hash is not None and file_hash is None:
        file_hash = md5_hash
        hash_algorithm = 'md5'
    datadir_base = os.path.expanduser(cache_dir)
    if not os.access(datadir_base, os.W_OK):
        datadir_base = os.path.join('/tmp', '.fawkes')
    datadir = os.path.join(datadir_base, cache_subdir)
    _makedirs_exist_ok(datadir)

    if untar:
        untar_fpath = os.path.join(datadir, fname)
        fpath = untar_fpath + '.tar.gz'
    else:
        fpath = os.path.join(datadir, fname)

    download = False
    if not os.path.exists(fpath):
        download = True

    if download:
        error_msg = 'URL fetch failure on {}: {} -- {}'
        dl_progress = None
        try:
            try:
                urlretrieve(origin, fpath, dl_progress)
            except HTTPError as e:
                raise Exception(error_msg.format(origin, e.code, e.msg))
            except URLError as e:
                raise Exception(error_msg.format(origin, e.errno, e.reason))
        except (Exception, KeyboardInterrupt) as e:
            if os.path.exists(fpath):
                os.remove(fpath)
            raise
        # ProgressTracker.progbar = None

    if untar:
        if not os.path.exists(untar_fpath):
            _extract_archive(fpath, datadir, archive_format='tar')
        return untar_fpath

    if extract:
        _extract_archive(fpath, datadir, archive_format)

    return fpath


def _extract_archive(file_path, path='.', archive_format='auto'):
    if archive_format is None:
        return False
    if archive_format == 'auto':
        archive_format = ['tar', 'zip']
    if isinstance(archive_format, six.string_types):
        archive_format = [archive_format]

    for archive_type in archive_format:
        if archive_type == 'tar':
            open_fn = tarfile.open
            is_match_fn = tarfile.is_tarfile
        if archive_type == 'zip':
            open_fn = zipfile.ZipFile
            is_match_fn = zipfile.is_zipfile

        if is_match_fn(file_path):
            with open_fn(file_path) as archive:
                try:
                    archive.extractall(path)
                except (tarfile.TarError, RuntimeError, KeyboardInterrupt):
                    if os.path.exists(path):
                        if os.path.isfile(path):
                            os.remove(path)
                        else:
                            shutil.rmtree(path)
                    raise
            return True
    return False


def _makedirs_exist_ok(datadir):
    if six.PY2:
        # Python 2 doesn't have the exist_ok arg, so we try-except here.
        try:
            os.makedirs(datadir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
    else:
        os.makedirs(datadir, exist_ok=True)  # pylint: disable=unexpected-keyword-arg

class FaceNotFoundError(Exception):
    pass
