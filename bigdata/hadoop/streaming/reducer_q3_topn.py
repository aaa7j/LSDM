import os
import sys
import heapq


def main():
    topn = int(os.environ.get("TOPN", "3"))
    cur_team = None
    heap = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        team_id = parts[0]
        try:
            points = int(parts[1])
        except Exception:
            continue
        season = parts[2]
        game_id = parts[3]
        if cur_team is None:
            cur_team = team_id
        if team_id != cur_team:
            # flush previous
            for p, s, g in sorted(heap, key=lambda x: (-x[0], x[2])):
                print("\t".join([cur_team, s, g, str(p)]))
            cur_team = team_id
            heap = []
        if len(heap) < topn:
            heapq.heappush(heap, (points, season, game_id))
        else:
            if points > heap[0][0]:
                heapq.heapreplace(heap, (points, season, game_id))
    if cur_team is not None:
        for p, s, g in sorted(heap, key=lambda x: (-x[0], x[2])):
            print("\t".join([cur_team, s, g, str(p)]))


if __name__ == "__main__":
    main()

