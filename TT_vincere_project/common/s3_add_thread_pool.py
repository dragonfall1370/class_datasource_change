import boto
import os
import shutil
import time
import re
import uuid
import pandas as pd
from boto.s3.connection import S3Connection
from multiprocessing.pool import ThreadPool
from queue import Queue
from threading import Thread
from .s3_connection_string import aws_access as access
from .s3_connection_string import aws_secret as secret
AWS_KEY = access
AWS_SECRET = secret
class Worker(Thread):


    """ Thread executing tasks from a given tasks queue """
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception as e:
                # An exception happened in this thread
                print(e)
            finally:
                # Mark this task as done, whether an exception happened or not
                self.tasks.task_done()


class ThreadPool:
    """ Pool of threads consuming tasks from a queue """
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """ Add a task to the queue """
        self.tasks.put((func, args, kargs))

    def map(self, func, args_list):
        """ Add a list of tasks to the queue """
        for args in args_list:
            self.add_task(func, args)

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()


def upload(filename, root, bucket, key, region_host=None, log=None):
    try:
        # AWS_SECRET=''
        if region_host:
            conn = S3Connection(aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, host=region_host)
        else:
            conn = S3Connection(aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
        bucket = conn.get_bucket(bucket)
        key = bucket.new_key(key + '/' + filename).set_contents_from_filename(os.path.join(root, filename))
        if log:
            log.info('successfully uploaded: %s' % os.path.join(root, filename))
        return key
    except Exception as e:
        if log:
            log.error(e)
        time.sleep(60)
        return upload(filename, root, bucket, key, log)


def upload1(real_filename, alter_filename, root, bucket_name, key, log=None, region_host=None, reup=0):
    """

    :param real_filename:
    :param alter_filename:
    :param root:
    :param bucket_name:
    :param key:
    :param log:
    :param region_host: must be not none if connect to frankfurt s3 server: REGION_HOST = 's3.eu-central-1.amazonaws.com'
    :return:
    """
    try:
        # AWS_SECRET=''
        if region_host:
            conn = S3Connection(aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, host=region_host)
        else:
            conn = S3Connection(aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)

        bucket = conn.get_bucket(bucket_name)
        key_is_existed = bucket.get_key(key + '/' + alter_filename)
        if not key_is_existed:
            key = bucket.new_key(key + '/' + alter_filename).set_contents_from_filename(os.path.join(str(root), str(real_filename)))
            if log:
                log.info('successfully uploaded: {}, renamed to {}'.format(os.path.join(root, real_filename), alter_filename))
            return key
        else:
            log.info('EXISTED FILE: {}, S3 FILENAME {}'.format(os.path.join(root, real_filename), alter_filename))
            return ''
    except Exception as e:
        if log:
            log.error(e)
            log.error(os.path.join(str(root), str(real_filename)))
            log.error('the file will be reuploaded')
        time.sleep(60)
        if reup < 10:
            return upload1(real_filename, alter_filename, root, bucket_name, key, log, reup=reup + 1)


def upload_multi_files_parallelism_1(upload_folder, bucket, key, log=None):
    """
    upload all files in the upload folder to s3. Files are uploaded in parallelism using Thread
    :param upload_folder: folder contains all files for uploading
    :param bucket: s3 bucket
    :param key: s3 key
    :return:
    """
    for root, dirs, files in os.walk(upload_folder):
        for idx, file in enumerate(files):
            t = Thread(target=upload, args=(file, root, bucket, key, log)).start()


def upload_multi_files_parallelism_1_1(df, file_name, alter_filename, root, bucket, key, log=None):
    """
    upload all files in the upload folder to s3. Files are uploaded in parallelism using Thread
    :param df: data frame contains atleast two columns: file, root - filename and root of the files are uploaded
    :param bucket: s3 bucket
    :param key: s3 key
    :return:
    """
    for index, row in df.iterrows():
        t = Thread(target=upload1, args=(row[file_name], row[alter_filename], row[root], bucket, key, log)).start()


def upload_multi_files_parallelism_1_2(df, file_name, alter_filename, root, bucket, key, log=None, region_host=None):
    """
    upload all files in the upload folder to s3. Files are uploaded in parallelism using Thread
    :param df: data frame contains atleast two columns: file, root - filename and root of the files are uploaded
    :param file_name real_filename
    :param bucket: s3 bucket
    :param key: s3 key
    :return:
    """
    # Instantiate a thread pool with 5 worker threads
    pool = ThreadPool(100)
    for index, row in df.iterrows():
        pool.add_task(upload1, row[file_name], row[alter_filename], row[root], bucket, key, log, region_host)
    pool.wait_completion()


def move_from_bucket_to_bucket(from_bucket, to_bucket, log):
    try:
        # AWS_SECRET=''
        conn = S3Connection(aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
        from_bucket = conn.get_bucket(from_bucket)
        to_bucket = conn.get_bucket(to_bucket)

        for k in from_bucket.list():
            # print(k.key.name)
            # copy stuff to your destination here
            to_bucket.copy_key(k.key.name, from_bucket.name, k.key.name)
            # then delete the source key
            # k.delete()

    except Exception as e:
        if log:
            log.error(e)
        time.sleep(60)
        return move_from_bucket_to_bucket(from_bucket, to_bucket, log)


def upload_multi_files_parallelism_2(upload_folder, bucket, key):
    """
    upload all files in the upload folder to s3. Files are uploaded in parallelism using ThreadPool
    :param upload_folder: folder contains all files for uploading
    :param bucket: s3 bucket
    :param key: s3 key
    :return:
    """
    for root, dirs, files in os.walk(upload_folder):
        for idx, file in enumerate(files):
            pool = ThreadPool(processes=16)
            pool.map(upload, file, root, bucket, key)


def upload_files_to_s3(upload_folder,
                       bucket='sin-vc-p1-file',
                       key='/tung.vincere.io/upload/file/1b9c5a18-ec95-4283-92ae-29600bc2d508/',
                       access_key='',
                       secret_key='+', overwrite=False, log=None):
    conn = boto.connect_s3(access_key, secret_key)
    bucket = conn.get_bucket(bucket)
    uploaded_fail = []
    total_files = sum([len(files) for r, d, files in os.walk(upload_folder)])
    total_uploaded_files = 0
    for root, dirs, files in os.walk(upload_folder):
        # print("root: %s" % root)
        # print("dirs: %s" % dirs)
        # print("files: %s" % files)
        # print("#--------------------------------")
        for idx, file in enumerate(files):
            total_uploaded_files += 1
            try:
                key_is_existed = False if overwrite else bucket.get_key(key + file)
                if not key_is_existed:
                    if log:
                        log.info('uploading: %i/%i - %s' % (total_uploaded_files, total_files, os.path.join(root, file)))
                    k = bucket.new_key(key + file)
                    k.set_contents_from_filename(os.path.join(root, file))
                # else:
                #     if log:
                #         log.debug('existed: %s' % os.path.join(root, file))

            except Exception as e:
                total_uploaded_files -= 1
                uploaded_fail.append(
                    {'file': file, 'root': root}
                )
                if log:
                    log.error(str(e))
    while len(uploaded_fail):
        e = uploaded_fail.pop(0)
        total_uploaded_files += 1
        file = e.get('file')
        root = e.get('root')
        try:
            key_is_existed = False if overwrite else bucket.get_key(key + file)
            if not key_is_existed:
                if log:
                    log.info('re-uploading: %i/%i - %s' % (total_uploaded_files, total_files, os.path.join(root, file)))
                k = bucket.new_key(key + file)
                k.set_contents_from_filename(os.path.join(root, file))
            # else:
            #     if log:
            #         log.debug('existed: %s' % os.path.join(root, file))

        except Exception as e:
            total_uploaded_files -= 1
            uploaded_fail.append(
                {'file': file, 'root': root}
            )
            if log:
                log.error(str(e))


def copy_files_to_specify_folder(upload_folder, df, to_folder):
    """

    :param upload_folder:
    :param df:
    :param to_folder:
    :return:
    """
    total_files = sum([len(files) for r, d, files in os.walk(upload_folder)])
    total_uploaded_files = 0
    for root, dirs, files in os.walk(upload_folder):
        # print("root: %s" % root)
        # print("dirs: %s" % dirs)
        # print("files: %s" % files)
        # print("#--------------------------------")
        for idx, file in enumerate(files):
            chkfile = df[df['file_name'] == file]
            if len(chkfile):  # check if file is exist in filter df_comp_files
                print("found: %s" % file)
                new_file_name = chkfile.iloc[0]['new_file_name']
                shutil.copyfile(os.path.join(root, file), os.path.join(to_folder, new_file_name))


def prepare_metadata_files(upload_folder):
    """
    :param upload_folder:
    :return:
    """
    try:
        temp = []
        for root, dirs, files in os.walk(upload_folder):
            for idx, file in enumerate(files):
                alter_file1 = re.sub(r"%|\$|&| |'|\[|\]|-|\+", '_', file)
                file_ext_pattern = r"\.[a-zA-Z0-9_ ]*(?!.*\.[a-zA-Z0-9_ ]*)"
                alter_file2 = ('%s%s') % (str(uuid.uuid4()), re.search(file_ext_pattern, file).group())
                temp.append({'file': file, 'root': root, 'alter_file1': alter_file1, 'alter_file2': alter_file2})
        return pd.DataFrame(temp)
    except Exception as ex:
        print(ex)
        print("%s/%s" % (root,file))