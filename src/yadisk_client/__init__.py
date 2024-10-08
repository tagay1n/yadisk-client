import hashlib
import os

import yadisk


class YaDisk(yadisk.YaDisk):

    def __init__(self, token: str):
        super().__init__(token=token)

    def upload_or_replace(self, local_file, remote_dir):
        """
        Uploads a file to the Yandex Disk if it does not exist or has a different hash.
        :param local_file: path to the file to upload
        :param remote_dir: path to the remote folder
        :return: path to the uploaded file
        """
        self.create_folders(remote_dir)
        file_name = os.path.basename(local_file).split('/')[-1]
        remote_path = f"{remote_dir}/{file_name}"

        meta = self.get_meta_or_none(remote_path, fields=["type", "md5"])
        if meta and meta['type'] == 'dir':
            raise ValueError(f"Cannot save file because directory with the same name already exists: {remote_path}")

        md5 = calculate_md5(local_file)
        if (not meta) or meta['md5'] != md5:
            # Upload the file if it does not exist or has a different hash
            self.upload(local_file, remote_path, overwrite=True)

        return remote_path, md5

    def get_public_download_link_by_remote_path(self, remote_path: str):
        pub_key = self.get_meta(remote_path, fields=['public_key'])['public_key']
        return self.get_public_download_link(pub_key)

    def create_folders(self, remote_dir: str):
        """
        Creates folders on the Yandex Disk if they do not exist.
        :param remote_dir: path to the remote folder
        """
        path_collector = ''
        for path_segment in remote_dir.split(os.sep):
            path_collector += path_segment + os.sep
            if not self.exists(path_collector):
                self.mkdir(path_collector)

    def get_meta_or_none(self, remote_path, **kwargs):
        """
        Returns metadata for the remote file or None if the file does not exist.
        :param remote_path: path to the remote file
        :param kwargs: any other parameters, accepted by :any:`Session.send_request()`
        :return: metadata for the remote file or None
        """
        try:
            return self.get_meta(remote_path, **kwargs)
        except yadisk.exceptions.PathNotFoundError:
            return None


def calculate_md5(file_path: str, buf_size: int = 2048):
    """
    Calculates MD5 hash of the file
    :param file_path: path to the file
    :param buf_size: size of the buffer to read the file
    :return: MD5 hash of the file
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(buf_size), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
