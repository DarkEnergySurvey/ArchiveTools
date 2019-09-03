""" Module for making tarballs

"""
import os
import tarfile

import archivetools.backup_util as bu


class DES_tarball(object):
    """ Class for generating a tarball from a directory
    """

    def __init__(self, args, items, data, util, path, size=0, md5sum=0, file_class=None, verify=False,
                 tarname=None):
        """ Parameters
            ----------
            args : dict
                Options to contol the processing
            items : list
                The name of the items to be tarred
            data : dict
                Dictionary containing directory and file information
            util : Util class instance
                used for logging
            path : str
                The full path of the directory to be tarred
            size : int
                The size of the tarball in bytes (Default is 0)
            md5sum : str
                The md5sum of the tarball (Default is 0)
            file_class : str
                The class of the data
        """
        if size != 0:
            self.tarfile = data
            self.tar_size = size
            self.md5sum = md5sum
            self.file_class = file_class
            return
        self.args = args
        self.items = items
        self.path = path
        self.util = util
        self.file_class = file_class
        self.tarfile = tarname
        self.tar_size = 0
        self.util.log(bu.Util.info, "=> Archiving: {0}".format(",".join(self.items)))

        while True:
            self.execute_tar()
            if verify:
                if bu.check_files(data, self.args['stgdir'], self.tarfile, self.util):
                    break
                os.remove(os.path.join(self.args['stgdir'], self.tarfile))
            else:
                break
        self.util.log(bu.Util.info, " ==> Generating md5sum for %s" % self.tarfile)
        self.md5sum = bu.generate_md5sum(os.path.join(self.args['stgdir'], self.tarfile))

    def ch_to_stage_dir(self):
        """ Method to change the directory to the staging dir
        """
        os.chdir(self.args['stgdir'])

    def get_filesize(self):
        """ Method to set the internal variable containing the size of the tarball
        """
        return self.tar_size

    def execute_tar(self):
        """ Method to generate the tar file
        """
        if not self.tarfile:
            self.tarfile = self.items[0].replace("/", ".") + ".tar"

        self.util.log(bu.Util.info, "===> Initiating Tar: {0}".format(self.tarfile))
        self.util.log(bu.Util.info, "===> Initiating Tar stgdir: {0}".format(self.args['stgdir']))
        self.ch_to_stage_dir()
        tar = tarfile.open(os.path.join(self.args['stgdir'], self.tarfile), "w", dereference=True)
        cwd = os.getcwd()
        os.chdir(self.path)
        for item in self.items:
            tar.add(item)
        os.chdir(cwd)
        tar.close()
        self.tar_size = os.path.getsize(os.path.join(self.args['stgdir'], self.tarfile))

        self.util.log(bu.Util.info, "===> Tar complete.  Size: {0}".format(self.tar_size))

    def get_md5sum(self):
        """ Method to return the md5sum

            Returns
            -------
            str containing the md5sum
        """
        return self.md5sum

    def get_tar_name(self):
        """ Method to return the name of the tar file

            Returns
            -------
            int, the size of the tar file in bytes
        """
        return self.tarfile
