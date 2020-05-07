import os
import os.path
import re
import sys


class Path():

    @classmethod
    def splitFullPathFileName(self, fullPathFileName):
        fullPathFileName = fullPathFileName.lower()
        driver = os.path.splitdrive(fullPathFileName)[0]
        path = os.path.split(fullPathFileName)[0]
        filename = os.path.split(fullPathFileName)[1]
        ext = os.path.splitext(fullPathFileName)[1]
        main = filename.strip(ext)
        sep = os.sep
        return {'driver': driver, 'sep': sep, 'path': path, 'filename': filename, 'main': main, 'ext': ext}

    @classmethod
    def filenameIsContains(self, fullPathFileName: str, strs):
        filename = Path.splitFullPathFileName(fullPathFileName).get('filename').lower()
        result = True
        for str in strs:
            result = filename.__contains__(str.lower()) and result
        return result

    @classmethod
    def outpathIsExist(self, outputPath):
        isExist = os.path.isdir(outputPath)
        if not isExist:
            os.makedirs(outputPath)
            print('The Path is not exist. Created (%s).' % outputPath)

    @classmethod
    def resource_path(self, relative_path: str):
        if getattr(sys, 'frozen', False):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.abspath(".")
        return os.path.join(base_path, relative_path)

    @classmethod
    def get_validate_filename(self, filename: str):
        # 检查文件名中是否含有以下非法字符：'/ \ : * ? " < > |'
        rstr = r"[\/\\\:\*\?\"\<\>\|]"
        # 非法字符全部替换为下划线
        new_file_name = re.sub(rstr, "_", filename)
        return new_file_name

    @classmethod
    def get_path_info(self):

        frozen = 'not'
        if getattr(sys, 'frozen', False):
            # we are running in a bundle
            frozen = 'ever so'
            bundle_dir = sys._MEIPASS
        else:
            # we are running in a normal Python environment
            bundle_dir = os.path.dirname(os.path.abspath(__file__))
        # print('we are', frozen, 'frozen')
        # print('bundle dir is', bundle_dir)
        # print('sys.argv[0] is', sys.argv[0])
        # print('sys.executable is', sys.executable)
        # print('os.getcwd is', os.getcwd())
        return {'frozen': frozen, 'bundle_dir': bundle_dir, 'argv0': sys.argv[0], 'executable': sys.executable,
                'cwd': os.getcwd()}


if __name__ == '__main__':
    path = Path()
    print(path.get_path_info())
