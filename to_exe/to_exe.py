from PyInstaller.__main__ import run


def mrp_web_to_only_exe():
    opts = [r'D:\plan_management\mrp_web\web\manage.py',
            '-F', r'--distpath=D:\to_exe_temp\mrp_web',
            r'--workpath=D:\to_exe_temp\history',
            r'--specpath=D:\to_exe_temp\build',
            r'-n=mrp_web']
    run(opts)


def py2exe_to_only_exe():
    opts = [r'E:\ad_zyp\to_exe\distribution_stations_folder_flow.py',
            '-F', '-w', r'--distpath=E:\ad_zyp\to_exe\files_flow',
            r'--workpath=E:\ad_zyp\to_exe\history',
            r'--specpath=E:\ad_zyp\to_exe\build',
            r'-n=files_flow',]
    run(opts)


if __name__ == "__main__":
    py2exe_to_only_exe()
