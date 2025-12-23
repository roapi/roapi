Обновление-установка Cmake на Ubuntu

```cmake --version```

```sudo apt remove cmake```

```wget https://github.com/Kitware/CMake/releases/download/v3.31.6/cmake-3.31.6-linux-x86_64.sh```

Копируем скрипт в /opt и делаем ```chmod +x /opt/cmake-3.*your_version*.sh```

```sudo bash /opt/cmake-3.*your_version*.sh```

```sudo ln -s /opt/cmake-3.*your_version*/bin/* /usr/local/bin```

```cmake --version```

