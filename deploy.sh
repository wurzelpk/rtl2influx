#!/bin/sh

cargo build --release
sudo service rtl2influx stop
sudo cp target/release/rtl2influx /usr/local/bin
sudo service rtl2influx start
journalctl -e -f -u rtl2influx
