import asyncio
import serialx
import pulsectl_asyncio
from pulsectl import PulseIndexError, PulseDisconnected, PulseOperationFailed, PulseEventFacilityEnum
import re

NUM_SLIDERS = 4

# Order still matters! Specific apps -> catch-all app -> master hardware device.
rules = [
    {"name": "Firefox", "keyword": "youtube", "slider": 2, "label" : "Youtube Firefox"},
    {"name": "discord", "keyword": None,      "slider": 3, "label" : "Discord"},
    {"special": "unmapped", "slider": 1, "label" : "Unmapped"},
    {"special": "default",   "slider": 0, "label" : "Master"} 
]

old_volume = [0 * NUM_SLIDERS]
sink_map = [{} for _ in range(NUM_SLIDERS)]


async def match_rule(obj, facility, pulse):
    if facility == PulseEventFacilityEnum.sink_input:
        app_name   = (obj.proplist.get('application.name', '') or '').lower()
        app_binary = (obj.proplist.get('application.process.binary', '') or '').lower() 
        media_name = (obj.proplist.get('media.name', '') or '').lower()

        for rule in rules:
            if rule.get("keyword") and rule["keyword"].lower() in media_name:
                return rule

        for rule in rules:
            if rule.get("name") and rule["name"].lower() in (app_name, app_binary):
                return rule

    elif facility == PulseEventFacilityEnum.sink:
        default_sink = (await pulse.server_info()).default_sink_name
        desc = (obj.proplist.get('device.description', '') or '').lower()

        for rule in rules:
            if obj.name == default_sink and rule.get("special") and rule["special"] == "default":
                return rule
            elif rule.get("name") and rule["name"].lower() == desc:
                return rule
            
    else:
        raise NotImplementedError
    

    #catch-all
    for rule in rules:
        if rule.get("special") == "unmapped":
            return rule
    
    return None # didn't match anything
    

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
    async with serialx.async_serial_for_url("/dev/pts/6", baudrate=9600) as serial:
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

                rule = await match_rule(stream, event.facility, pulse)
                target_slider = rule.get("slider") if rule else None


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
                        rule = await match_rule(sink, event.facility, pulse)
                        slider_idx = rule.get("slider") if rule else None
                        if slider_idx is not None:
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
        for stream in await pulse.sink_input_list():
            rule = await match_rule(stream, PulseEventFacilityEnum.sink_input, pulse)
            slider_idx = rule.get("slider") if rule else None
            if slider_idx is not None:
                sink_map[slider_idx][stream.index] = stream
                print(f"Startup: mapped slider {slider_idx} '{rule['label']}' to app stream {stream.index}")

        for sink in await pulse.sink_list():
            rule = await match_rule(sink, PulseEventFacilityEnum.sink, pulse)
            slider_idx = rule.get("slider") if rule else None
            if slider_idx is not None:
                sink_map[slider_idx][sink.index] = sink
                print(f"Startup: mapped slider {slider_idx} '{rule['label']}' to default hardware sink {sink.index}")

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