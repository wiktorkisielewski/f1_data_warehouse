import sys
import time

# ANSI colors
GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"
BLUE = "\033[94m"
RED = "\033[91m"
RESET = "\033[0m"
BOLD = "\033[1m"

BAR_LENGTH = 30

_header_printed = False
_first_render_done = False
BLOCK_LINES = 6


def clear_block():
    sys.stdout.write(f"\033[{BLOCK_LINES}A")
    for _ in range(BLOCK_LINES):
        sys.stdout.write("\033[K\n")
    sys.stdout.write(f"\033[{BLOCK_LINES}A")


def render_progress(current_season_index, total_seasons, total_races):
    global _header_printed, _first_render_done

    progress_ratio = current_season_index / total_seasons
    percent = int(progress_ratio * 100)

    filled = int(BAR_LENGTH * progress_ratio)
    bar = "█" * filled + "░" * (BAR_LENGTH - filled)

    laps_estimate = total_races * 60
    distance_estimate = laps_estimate * 5

    # Print header once
    if not _header_printed:
        print()
        print(f"{BOLD}🚀 F1 Historical Ingestion Progress{RESET}\n")
        _header_printed = True

    # Only clear AFTER first render
    if _first_render_done:
        clear_block()

    print(f"{GREEN}Progress:{RESET}  [{GREEN}{bar}{RESET}] {percent}%")
    print(f"{CYAN}Seasons:{RESET}   {current_season_index} / {total_seasons}")
    print(f"{YELLOW}Races:{RESET}     {total_races:,} synced")
    print(f"{MAGENTA}Laps:{RESET}      ~{laps_estimate:,} estimated")
    print(f"{BLUE}Distance:{RESET}  ~{distance_estimate:,} km covered")
    print()

    sys.stdout.flush()
    _first_render_done = True

def reset_progress_display():
    global _first_render_done, _header_printed
    _first_render_done = False
    _header_printed = False

def render_cooldown(seconds):
    print(f"\n{RED}⚠ API rate limit reached{RESET}")
    print()
    for remaining in range(seconds, 0, -1):
        mins, secs = divmod(remaining, 60)

        # Move cursor up 1 line (cooldown line)
        sys.stdout.write("\033[1A")
        sys.stdout.write("\033[K")

        print(
            f"{RED}Cooling down:{RESET} {mins:02d}:{secs:02d} remaining..."
        )

        sys.stdout.flush()
        time.sleep(1)

    # Clear cooldown line after finishing
    sys.stdout.write("\033[1A")
    sys.stdout.write("\033[K")
    sys.stdout.flush()

    print()