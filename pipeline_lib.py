# pipeline_lib.py
# -*- coding: utf-8 -*-

import os
import json
import math
import itertools
import shutil
from collections import defaultdict, OrderedDict


# ===============================================================
# ===================== 第一阶段：组合 + OBB 冲突过滤 =====================
# ===============================================================

def normalize(vx, vy):
    norm = math.hypot(vx, vy)
    if norm < 1e-6:
        return 1.0, 0.0
    return vx / norm, vy / norm


def get_vehicle_corners(pos, direction, car_length, car_width):
    x, y = pos[0], pos[1]
    dx, dy = normalize(direction[0], direction[1])

    fx, fy = dx, dy
    sx, sy = -dy, dx

    hl = car_length / 2.0
    hw = car_width / 2.0

    return [
        (x + fx * hl + sx * hw, y + fy * hl + sy * hw),
        (x + fx * hl - sx * hw, y + fy * hl - sy * hw),
        (x - fx * hl - sx * hw, y - fy * hl - sy * hw),
        (x - fx * hl + sx * hw, y - fy * hl + sy * hw),
    ]


def project_polygon(axis, polygon):
    dots = [axis[0] * p[0] + axis[1] * p[1] for p in polygon]
    return min(dots), max(dots)


def overlap(p1, p2):
    return not (p1[1] < p2[0] or p2[1] < p1[0])


def obb_intersect(c1, c2):
    axes = []
    for corners in (c1, c2):
        for i in range(4):
            x1, y1 = corners[i]
            x2, y2 = corners[(i + 1) % 4]
            edge = (x2 - x1, y2 - y1)
            axis = (-edge[1], edge[0])
            norm = math.hypot(axis[0], axis[1])
            axes.append((axis[0] / norm, axis[1] / norm))

    for axis in axes:
        if not overlap(
            project_polygon(axis, c1),
            project_polygon(axis, c2),
        ):
            return False
    return True


def stage1_generate_combinations(
    data_dir,
    output_dir,
    group_num,
    pick_per_group,
    car_length,
    car_width,
):
    os.makedirs(output_dir, exist_ok=True)

    files = [
        f for f in os.listdir(data_dir)
        if f.endswith(".json") and f[:6].isdigit()
    ]

    file_infos = []
    for fname in files:
        with open(os.path.join(data_dir, fname), "r", encoding="utf-8") as f:
            data = json.load(f)
        file_infos.append({"name": fname, "count": len(data)})

    file_infos.sort(key=lambda x: x["count"])

    print("文件按轨迹数排序：")
    for info in file_infos:
        print(f"  {info['name']} : {info['count']}")

    groups = [[] for _ in range(group_num)]
    for idx, info in enumerate(file_infos):
        gid = idx * group_num // len(file_infos)
        groups[gid].append(info["name"])

    print("\n分组结果：")
    for i, g in enumerate(groups):
        print(f"  组 {i+1} (共 {len(g)} 个): {g}")

    group_choices = []
    for g in groups:
        if len(g) < pick_per_group:
            raise ValueError("组内文件数不足")
        group_choices.append(list(itertools.combinations(g, pick_per_group)))

    print("\n开始处理组合 ...")

    for combo in itertools.product(*group_choices):
        selected_files = [f for group in combo for f in group]

        merged_data = OrderedDict()
        traj_len = {}
        vid = 0

        for fname in selected_files:
            with open(os.path.join(data_dir, fname), "r", encoding="utf-8") as f:
                data = json.load(f)
            for _, traj in data.items():
                merged_data[str(vid)] = traj
                traj_len[str(vid)] = len(traj)
                vid += 1

        ts_map = defaultdict(list)
        for vid, traj in merged_data.items():
            for frame in traj:
                ts_map[frame[2]].append((vid, frame[0], frame[1]))

        remove_ids = set()

        for vehicles in ts_map.values():
            n = len(vehicles)
            for i in range(n):
                id1, pos1, dir1 = vehicles[i]
                if id1 in remove_ids:
                    continue
                c1 = get_vehicle_corners(pos1, dir1, car_length, car_width)
                for j in range(i + 1, n):
                    id2, pos2, dir2 = vehicles[j]
                    if id2 in remove_ids:
                        continue
                    c2 = get_vehicle_corners(pos2, dir2, car_length, car_width)
                    if obb_intersect(c1, c2):
                        if traj_len[id1] >= traj_len[id2]:
                            remove_ids.add(id2)
                        else:
                            remove_ids.add(id1)

        filtered = OrderedDict()
        for new_idx, (vid, traj) in enumerate(
            (v for v in merged_data.items() if v[0] not in remove_ids)
        ):
            filtered[str(new_idx)] = traj

        combo_name = "-".join([f[:6] for f in selected_files])
        out_path = os.path.join(output_dir, f"{combo_name}.json")

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(filtered, f, indent=2)

        print(
            f"[{combo_name}] 原始轨迹: {len(merged_data)}, "
            f"删除轨迹: {len(remove_ids)}, "
            f"保留轨迹: {len(filtered)}"
        )

    print("\n✅ 第一阶段完成")


# ===============================================================
# ===================== 第二阶段：主体车裁剪 =====================
# ===============================================================

def stage2_subject_filter(
    input_dir,
    output_dir,
    min_vehicle_num,
    min_duration,
):
    os.makedirs(output_dir, exist_ok=True)

    for fname in os.listdir(input_dir):
        if not (fname.endswith(".json") and fname[:4].isdigit()):
            continue

        json_path = os.path.join(input_dir, fname)
        name = os.path.splitext(fname)[0]
        out_dir = os.path.join(output_dir, name)
        os.makedirs(out_dir, exist_ok=True)

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        time_ranges = {}
        for vid, frames in data.items():
            times = [fr[2] for fr in frames]
            time_ranges[vid] = (min(times), max(times))

        for subject_id, (t0, t1) in time_ranges.items():
            kept = {}
            for vid, frames in data.items():
                if time_ranges[vid][0] <= t0 and time_ranges[vid][1] >= t1:
                    kept[vid] = frames

            if len(kept) <= min_vehicle_num:
                continue

            duration = t1 - t0
            if duration <= min_duration:
                continue

            reindexed = OrderedDict()
            reindexed["0"] = kept[subject_id]
            nid = 1
            for vid, frames in kept.items():
                if vid != subject_id:
                    reindexed[str(nid)] = frames
                    nid += 1

            out_name = f"{duration}_{len(reindexed)}.json"
            out_path = os.path.join(out_dir, out_name)

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(reindexed, f, indent=2, ensure_ascii=False)

            print(
                f"[{name} | 主体 {subject_id}] "
                f"时间长度 {duration} | 保留 {len(reindexed)} 辆"
            )

    print("\n✅ 第二阶段完成")


# ===============================================================
# ===================== 第三阶段：最终筛选 =====================
# ===============================================================

def stage3_collect(
    input_dir,
    output_dir,
    min_duration,
    min_traj_num,
):
    os.makedirs(output_dir, exist_ok=True)

    matched = 0
    for root, _, files in os.walk(input_dir):
        for fname in files:
            if not fname.endswith(".json"):
                continue

            name = os.path.splitext(fname)[0]
            parts = name.split("_")
            if len(parts) != 2:
                continue

            try:
                duration = int(parts[0])
                traj_num = int(parts[1])
            except ValueError:
                continue

            if duration >= min_duration and traj_num >= min_traj_num:
                src = os.path.join(root, fname)
                parent = os.path.basename(root)
                dst = os.path.join(output_dir, f"{parent}_{fname}")
                shutil.copy2(src, dst)
                matched += 1

    print("筛选条件：")
    print(f"  主车时长 ≥ {min_duration}")
    print(f"  轨迹数量 ≥ {min_traj_num}")
    print(f"\n符合条件文件数：{matched}")
    print("\n✅ 第三阶段完成")
