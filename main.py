import asyncio
import serialx
import pulsectl_asyncio
from pulsectl import PulseIndexError, PulseDisconnected, PulseOperationFailed
import re

NUM_SLIDERS = 4

# Order still matters! Specific apps -> catch-all app -> master hardware device.
rules = [
    {"name": "Firefox", "keyword": "youtube", "slider": 2, "label" : "Youtube Firefox"},
    {"name": "discord", "keyword": None,      "slider": 3, "label" : "Discord"},
    {"type": "unmapped", "slider": 1, "label" : "Unmapped"},
    {"type": "master",   "slider": 0, "label" : "Master"} 
]

old_volume = "|".join(["0"] * NUM_SLIDERS)
sink_map = [{} for _ in range(NUM_SLIDERS)]


def match_rule(stream, rule, is_sink=False):
    # master can only match a sink, might change later to allow any arbitrary sink
    if rule.get("type") == "master":
        return is_sink  
        
    # sinks can't match for applications which are the only thing below
    if is_sink:
        return False 
        
    # catch-all
    if rule.get("type") == "unmapped":
        return True   
    
    app_name   = (stream.proplist.get('application.name', '') or '').lower()
    app_binary = (stream.proplist.get('application.process.binary', '') or '').lower() 
    media_name = (stream.proplist.get('media.name', '') or '').lower()

    # Check if the basic application matches
    app_matches = rule["name"].lower() in (app_name, app_binary)
    if not app_matches:
        return False

    # check if its both an app and a keyword
    if rule["keyword"]:
        return rule["keyword"].lower() in media_name


    return True #app but not keyword


async def setvolume(data, pulse):
    global old_volume
    old_volume = []
    for i, volume in enumerate(data.split("|")):
        old_volume.append(int(volume))
        if i >= len(sink_map):
            continue

        for idx, obj in list(sink_map[i].items()):
            try:
                await pulse.volume_set_all_chans(obj, int(volume) / 100)
            except (PulseIndexError, PulseOperationFailed):
                sink_map[i].pop(idx, None)

async def handle_sliders(pulse):
    async with serialx.async_serial_for_url("/dev/pts/9", baudrate=9600) as serial:
        while True:
            data = await serial.readline()
            data = data.decode("utf-8").strip()
            if re.fullmatch(r'^(100|\d{1,2})(?:\|(100|\d{1,2}))*$', data):
                await setvolume(data, pulse)


async def handle_subscription(pulse):
    try:
        async for event in pulse.subscribe_events('all'):
            if event.facility not in ('sink', 'sink_input'):
                continue

            # ==========================================
            # CASE A: APPLICATION STREAM EVENTS
            # ==========================================
            if event.facility == 'sink_input':
                if event.t == 'remove':
                    for i in range(len(sink_map)):
                        sink_map[i].pop(event.index, None)
                    continue

                try:
                    stream = await pulse.sink_input_info(event.index)
                except PulseIndexError:
                    continue

                # 1. Track current and target sliders
                current_slider = None
                for i in range(len(sink_map)):
                    if stream.index in sink_map[i]:
                        current_slider = i
                        break

                target_slider = None
                for rule in rules:
                    if match_rule(stream, rule, is_sink=False):
                        target_slider = rule["slider"]
                        break

                # 2. Smart Routing & Delta Checking Logic
                if target_slider is not None:
                    target_vol = old_volume[target_slider] / 100 if target_slider < len(old_volume) else 0.0

                    if current_slider == target_slider:
                        # The app is on the right slider. Did a human change it externally?
                        # (Using a delta threshold of 0.02 to absorb float rounding discrepancies)
                        if abs(stream.volume.value_flat - target_vol) > 0.02:
                            # EXTERNAL OVERRIDE DETECTED! Force it back to match the physical slider
                            try:
                                await pulse.volume_set_all_chans(stream, target_vol)
                            except (PulseIndexError, PulseOperationFailed):
                                sink_map[target_slider].pop(stream.index, None)
                        else:
                            # It's just our own echo. Update object silently, don't re-fire volume command.
                            sink_map[target_slider][stream.index] = stream
                    else:
                        # It's a brand new app or it hopped rules (e.g. to YouTube)
                        if current_slider is not None:
                            sink_map[current_slider].pop(stream.index, None)
                        
                        sink_map[target_slider][stream.index] = stream
                        
                        try:
                            await pulse.volume_set_all_chans(stream, target_vol)
                        except (PulseIndexError, PulseOperationFailed):
                            sink_map[target_slider].pop(stream.index, None)

            # ==========================================
            # CASE B: HARDWARE DEVICE EVENTS (Master)
            # ==========================================
            elif event.facility == 'sink':
                if event.t in ('change', 'new'):
                    try:
                        server_info = await pulse.server_info()
                        sink = await pulse.sink_info(event.index)
                    except PulseIndexError:
                        continue
                    
                    if sink.name == server_info.default_sink_name:
                        for rule in rules:
                            if match_rule(sink, rule, is_sink=True):
                                slider_idx = rule["slider"]
                                target_vol = old_volume[slider_idx] / 100 if slider_idx < len(old_volume) else 0.0
                                
                                if sink.index in sink_map[slider_idx]:
                                    # Master sink already tracked. Did someone use keyboard media keys or OS UI?
                                    if abs(sink.volume.value_flat - target_vol) > 0.02:
                                        # Snap master hardware volume back to physical potentiometer placement
                                        try:
                                            await pulse.volume_set_all_chans(sink, target_vol)
                                        except (PulseIndexError, PulseOperationFailed):
                                            sink_map[slider_idx].pop(sink.index, None)
                                    else:
                                        sink_map[slider_idx][sink.index] = sink
                                else:
                                    # Freshly connected or newly selected default device, initialize it
                                    sink_map[slider_idx].clear()
                                    sink_map[slider_idx][sink.index] = sink
                                    try:
                                        await pulse.volume_set_all_chans(sink, target_vol)
                                    except (PulseIndexError, PulseOperationFailed):
                                        sink_map[slider_idx].pop(sink.index, None)
                                break

    except PulseDisconnected:
        pass
    except Exception as e:
        print(f"handle_subscription crashed: {e!r}")
        raise


async def main():
    async with pulsectl_asyncio.PulseAsync('custom-mixer-daemon') as pulse:
        # 1. Map existing application streams on startup
        for stream in await pulse.sink_input_list():
            for rule in rules:
                if match_rule(stream, rule, is_sink=False):
                    slider_idx = rule["slider"]
                    sink_map[slider_idx][stream.index] = stream
                    print(f"Startup: mapped slider {slider_idx} '{rule['label']}' to app stream {stream.index}")
                    break

        # 2. Map the active default hardware sink on startup
        server_info = await pulse.server_info()
        for sink in await pulse.sink_list():
            if sink.name == server_info.default_sink_name:
                for rule in rules:
                    if match_rule(sink, rule, is_sink=True):
                        slider_idx = rule["slider"]
                        sink_map[slider_idx][sink.index] = sink
                        print(f"Startup: mapped slider {slider_idx} '{rule['label']}' to default hardware sink {sink.index}")
                        break

        sub_task = asyncio.create_task(handle_subscription(pulse))
        try:
            await asyncio.gather(handle_sliders(pulse), sub_task)
        except Exception:
            sub_task.cancel()
            try:
                await sub_task
            except (asyncio.CancelledError, PulseDisconnected):
                pass
            raise


if __name__ == '__main__':
    asyncio.run(main())

# socat -d -d pty,raw,echo=0 pty,raw,echo=0
# to start testing port 
# echo "50|100|50|80" > /dev/pts/10
# to write to whatever testing port