from setuptools import setup, find_packages

setup(
    name='marketmaker', # 包名称，之后如果上传到了pypi，则需要通过该名称下载
    version='1.0.0', # version只能是数字，还有其他字符则会报错
    keywords=('setup', 'marketmaker'),
    description='marketmaker for bitasset',
    long_description='',
    license='MIT', # 遵循的协议
    install_requires=[], # 这里面填写项目用到的第三方依赖
    author='Yaogong Zhang',
    author_email='jasonwyse.nku@gmail.com',
    packages=find_packages(), # 项目内所有自己编写的库
    platforms='any',
    url='' ,# 项目链接,
    include_package_data = True,

)