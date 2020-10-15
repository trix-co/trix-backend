# from __future__ import absolute_import
# from __future__ import division
# from __future__ import print_function

import argparse
import glob
import logging
import os
import sys
import time

import tensorflow as tf

logging.getLogger('tensorflow').disabled = True

import numpy as np
from fawkes.differentiator import FawkesMaskGeneration
from utils import load_extractor, init_gpu, select_target_label, dump_image, reverse_process_cloaked, \
    Faces, filter_image_paths

from fawkes.align_face import aligner
from fawkes.utils import get_file
import datetime


def generate_cloak_images(protector, image_X, target_emb=None):
    cloaked_image_X = protector.attack(image_X, target_emb)
    return cloaked_image_X


def check_imgs(imgs):
    if np.max(imgs) <= 1 and np.min(imgs) >= 0:
        imgs = imgs * 255.0
    elif np.max(imgs) <= 255 and np.min(imgs) >= 0:
        pass
    else:
        raise Exception("Image values ")
    return imgs


class Fawkes(object):
    def __init__(self, feature_extractor, gpu, batch_size):

        self.feature_extractor = feature_extractor
        self.gpu = gpu
        self.batch_size = batch_size
        global sess
        sess = init_gpu(gpu)
        global graph
        graph = tf.get_default_graph()

        model_dir = os.path.join(os.path.expanduser('~'), '.fawkes')
        if not os.path.exists(os.path.join(model_dir, "mtcnn.p.gz")):
            os.makedirs(model_dir, exist_ok=True)
            get_file("mtcnn.p.gz", "http://sandlab.cs.uchicago.edu/fawkes/files/mtcnn.p.gz", cache_dir=model_dir,
                     cache_subdir='')

        self.fs_names = [feature_extractor]
        if isinstance(feature_extractor, list):
            self.fs_names = feature_extractor

        self.aligner = aligner(sess)
        self.feature_extractors_ls = [load_extractor(name) for name in self.fs_names]

        global protector
        global protector_param

        mode='low'
        th=0.04
        sd=1e9
        lr=10
        max_step=500
        batch_size=1
        format='png'
        separate_target=True
        debug=False
        th, max_step, lr = self.mode2param(mode)

        protector_param = "-".join([str(x) for x in [mode, th, sd, lr, max_step, batch_size, format, separate_target, debug]])

        print('h', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        print('i', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        protector = FawkesMaskGeneration(sess, self.feature_extractors_ls,
                                          batch_size=batch_size,
                                          mimic_img=True,
                                          intensity_range='imagenet',
                                          initial_const=sd,
                                          learning_rate=lr,
                                          max_iterations=max_step,
                                          l_threshold=th,
                                          verbose=1 if debug else 0,
                                          maximize=False,
                                          keep_final=False,
                                          image_shape=(224, 224, 3))


        print('j', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        with sess.as_default():
            print("hello")



    def mode2param(self, mode):
        if mode == 'low':
            th = 0.003
            max_step = 45
            lr = 20
        elif mode == 'mid':
            th = 0.005
            max_step = 120
            lr = 15
        elif mode == 'high':
            th = 0.008
            max_step = 600
            lr = 10
        elif mode == 'ultra':
            if not tf.test.is_gpu_available():
                print("Please enable GPU for ultra setting...")
                sys.exit(1)
            th = 0.01
            max_step = 1000
            lr = 10
        else:
            raise Exception("mode must be one of 'low', 'mid', 'high', 'ultra', 'custom'")
        return th, max_step, lr


    def run_protection(self, image_paths, mode='low', th=0.04, sd=1e9, lr=10, max_step=500, batch_size=1, format='png',
                       separate_target=True, debug=False):



        image_paths, loaded_images = filter_image_paths(image_paths)

        if not image_paths:
            raise Exception("No images in the directory")

        faces = Faces(image_paths, loaded_images, self.aligner, verbose=1)
        print('d', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        original_images = faces.cropped_faces
        original_images = np.array(original_images)

        print('e', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        if separate_target:
            target_embedding = []
            for org_img in original_images:
                org_img = org_img.reshape([1] + list(org_img.shape))
                tar_emb = select_target_label(org_img, self.feature_extractors_ls, self.fs_names)
                target_embedding.append(tar_emb)
            target_embedding = np.concatenate(target_embedding)
            print('f', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        else:
            target_embedding = select_target_label(original_images, self.feature_extractors_ls, self.fs_names)
            print('g', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))


        protected_images = generate_cloak_images(protector, original_images,
                                                 target_emb=target_embedding)
        print('k', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        faces.cloaked_cropped_faces = protected_images
        print('l', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        cloak_perturbation = reverse_process_cloaked(protected_images) - reverse_process_cloaked(
            original_images)
        print('m', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        final_images = faces.merge_faces(cloak_perturbation)
        print('n', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        for p_img, path in zip(final_images, image_paths):
            file_name = "{}_{}_cloaked.{}".format(".".join(path.split(".")[:-1]), mode, format)
            dump_image(p_img, file_name, format=format)
        print('o', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        print("Done!")
        return None


def main(*argv):
    print('a', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    if not argv:
        argv = list(sys.argv)

    try:
        import signal
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    except Exception as e:
        pass

    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', '-d', type=str,
                        help='directory that contain images for cloaking', default='imgs/')

    parser.add_argument('--gpu', '-g', type=str,
                        help='GPU id', default='0')

    parser.add_argument('--mode', '-m', type=str,
                        help='cloak generation mode', default='low')
    parser.add_argument('--feature-extractor', type=str,
                        help="name of the feature extractor used for optimization",
                        default="high_extract")

    parser.add_argument('--th', type=float, default=0.01)
    parser.add_argument('--max-step', type=int, default=1000)
    parser.add_argument('--sd', type=int, default=1e9)
    parser.add_argument('--lr', type=float, default=2)

    parser.add_argument('--batch-size', type=int, default=1)
    parser.add_argument('--separate_target', action='store_true')
    parser.add_argument('--debug', action='store_true')

    parser.add_argument('--format', type=str,
                        help="final image format",
                        default="png")

    args = parser.parse_args(argv[1:])

    assert args.format in ['png', 'jpg', 'jpeg']
    if args.format == 'jpg':
        args.format = 'jpeg'

    image_paths = glob.glob(os.path.join(args.directory, "*"))
    image_paths = [path for path in image_paths if "_cloaked" not in path.split("/")[-1]]
    print('b', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    protector = Fawkes(args.feature_extractor, args.gpu, args.batch_size)
    print('c', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    protector.run_protection(image_paths, mode=args.mode, th=args.th, sd=args.sd, lr=args.lr, max_step=args.max_step,
                             batch_size=args.batch_size, format=args.format,
                             separate_target=args.separate_target, debug=args.debug)
    print('z', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))


if __name__ == '__main__':
    main(*sys.argv)
