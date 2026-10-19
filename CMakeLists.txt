cmake_minimum_required(VERSION 3.20.0)

find_package(Zephyr REQUIRED)
project(b_l4s5i_iot01a_wifi_app)

# 修正：將目標名稱從 b_l4s5i_iot01a_wifi_app 更改為 app
target_sources(app PRIVATE src/main.c)