
import logging
import os
import shutil
import tarfile
import urllib.request

import lxml.html
from scipy.io import wavfile
from tqdm import *

logger = logging.getLogger()

voxforge_url = 'http://www.repository.voxforge1.org/downloads/Russian/Trunk/Audio/Main/8kHz_16bit/'
voxforge_folder = '/home/n_chernetsov/WORK/asr-kws/data/VoxForge'


def download_voxforge_data(path):
    path = os.path.abspath(path)

    print('Saving to ' + path + '...')

    if not os.path.exists(path):
        os.mkdir(path)

    url = voxforge_url

    connection = urllib.request.urlopen(url)

    dom = lxml.html.fromstring(connection.read())

    files = []
    for link in dom.xpath('//a/@href'):
        if link.endswith('.tgz'):
            files.append(link)

    print('Found ' + str(len(files)) + ' files (sessions)...')

    skipped = 0
    dled = 0
    for f in tqdm(files):
        dl = path + '/' + f
        u = url + f

        if not os.path.exists(dl):
            dled += 1
            try:
                # print("Download {}".format(dl))
                urllib.request.urlretrieve(u, dl)
            except urllib.request.ContentTooShortError as e:
                print(e.strerror)
                print('Error downloading! Retrying...')
        else:
            skipped += 1

    print('Downloaded ' + str(dled) + ' files...')
    print('Skipped (already existing) ' + str(skipped) + ' files...')


class CorpusSession:
    """ A class describing a single session of the corpus.
        Properties:
            props(dictionary): properties of the session as written in the readme file
                saved as key->value dictionary
            prompts(dictionary): list of prompts (words) for individual utterances saved
                as file(string)->prompt(string list) dictionary
            data(dictonary): list of audio recordings of the same utterances as above saved
                as file(string)->audio(numpy array) dictionary
        Note: due to files being saved in more than one format (WAV and FLAC), scikits.audiolab
        is used to load the data. Samples are stored as numpy.int16 datatype.
    """

    def __init__(self):
        self.props = {}
        self.prompts = {}
        self.data = {}
        self.is_normal_file = True  # Булевский флаг, показывающий, нормально ли прочитался файл


def load_file(path):
    """ Loads a single session from a TGZ archive.
        Args:
            path(string): path to the tgz archive

        Returns:
            CorpusSession: corpus session object with all the data loaded
    """
    logger.info("Load data from VoxForge file: {}".format(path))

    tar_archive = tarfile.open(path)

    try:
        names = tar_archive.getnames()
    except EOFError as e:
        logger.error("Exception while read tar file: {}".format(e))
        return None

    readme_path = [name for name in names if name.endswith('README') or name.endswith('readme')][0]

    readme_file = tar_archive.extractfile(readme_path)

    props = {}
    for line in readme_file:
        str_line = line.decode("utf-8")
        t = str_line.split(':')
        if (len(t) == 2):
            props[t[0].strip()] = t[1].strip()

    props['Path'] = readme_path.split('/')[0]

    prompts_path = [name for name in names if name.endswith('PROMPTS') or name.endswith('prompts')][0]

    prompts_file = tar_archive.extractfile(prompts_path)

    prompts = {}
    for line in prompts_file:
        str_line = line.decode("utf-8")
        t = str_line.split()
        prompt = t[0].split('/')[-1]
        prompts[prompt] = t[1:]

    type = 'flac'
    if any(item.endswith('/wav') for item in names):
        type = 'wav'

    prompts_file = props['Path']
    data = {}
    for prompt in prompts:
        try:
            ft = tar_archive.extractfile(prompts_file + '/' + type + '/' + prompt + '.' + type)
        except KeyError:
            continue

        shutil.copyfileobj(ft, open('temp', 'wb'))  # копируем из файла ft в файл temp
        wav = wavfile.read('temp')
        data[prompt] = wav[1]  # звуковая волна из аудиофайла

    ret = CorpusSession()
    ret.props = props
    ret.prompts = prompts
    ret.data = data

    try:
        os.remove('temp')
    except OSError:
        logger.info("File {} has problem to read. Skip it".format(path))
        ret.is_normal_file = False
        pass

    tar_archive.close()
    return ret


# Загружаем все файлы и аудио в один датасет
def get_voxforge_dataset():
    dataset = []
    for path, subfolders, files in os.walk(voxforge_folder):
        for file in files:
            file_path = path + '/' + file
            corpus_session = load_file(file_path)
            if corpus_session is not None and corpus_session.is_normal_file:
                audio = corpus_session.data
                prompts = corpus_session.prompts
                for id, text in prompts.items():
                    if id in audio.keys():
                        wav_fragment = audio[id]
                        text_lowercase = [word.lower() for word in text]
                        text = ' '.join(text_lowercase)
                        dataset.append((wav_fragment, text))
    return dataset
