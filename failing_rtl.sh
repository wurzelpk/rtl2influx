#!/bin/sh

echo "$0 Running.  Arguments: $@"

    epoch=$(date +%s)
    temperature=$(echo "scale = 3 ; sin(${epoch} / 60) * 10.0 + 20" | bc -l)
    echo '{"time" : "' "${date}" '", "model" : "Acurite-Tower", "id" : 10956, "A": "b", "channel" : "A", "battery_ok" : 1, "temperature_C" : ' $temperature ', "humidity" : 55, "mic" : "CHECKSUM"}'
    sleep 5

exit 42
