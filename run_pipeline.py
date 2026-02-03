# run_pipeline.py
# -*- coding: utf-8 -*-

from pipeline_lib import (
    stage1_generate_combinations,
    stage2_subject_filter,
    stage3_collect,
)

# ===================== 全局路径配置 =====================
DATA_DIR = "./data"              # 原始轨迹 JSON 所在目录
STAGE1_OUT = "./data/outputs"    # 第一阶段输出目录（组合 + 冲突过滤）
STAGE2_OUT = "./data_deal_1"     # 第二阶段输出目录（主体车裁剪）
FINAL_OUT = "./num"              # 第三阶段输出目录（最终筛选结果）

# ===================== 第一阶段参数 =====================
GROUP_NUM = 5                    # 将所有文件按轨迹数量分成的组数
PICK_PER_GROUP = 1               # 每组选择的文件数量
CAR_LENGTH = 5                   # 车辆长度（米）
CAR_WIDTH = 2                    # 车辆宽度（米）

# ===================== 第二阶段参数 =====================
MIN_VEHICLE_NUM = 5              # 保留的最少车辆数量（含主体车）
MIN_DURATION_STAGE2 = 5000       # 主体车的最小时间跨度

# ===================== 第三阶段参数 =====================
MIN_DURATION_STAGE3 = 20000      # 最终保留场景的最小主体车时长
MIN_TRAJ_NUM = 10                # 最终保留场景的最小轨迹数量


def main():
    """
    主流程函数。

    本函数按照固定顺序执行三个阶段的数据处理流程。
    每个阶段的输入来自前一阶段的输出。
    所有阶段的逻辑相互独立，但数据上存在依赖关系。
    """

    # ---------------------------------------------------
    # 第一阶段：轨迹组合与空间冲突过滤
    #
    # 功能：
    # 1. 读取 DATA_DIR 中的原始 JSON 文件
    # 2. 按轨迹数量排序并分组
    # 3. 从每一组中选取文件并进行组合
    # 4. 合并轨迹并在同一时间戳下进行 OBB 碰撞检测
    # 5. 对发生碰撞的车辆，保留轨迹更长的一方
    # 6. 对剩余轨迹重新编号并保存
    #
    # 输出：
    # 每个组合生成一个新的 JSON 文件
    # 文件保存在 STAGE1_OUT 目录
    # ---------------------------------------------------
    stage1_generate_combinations(
        data_dir=DATA_DIR,
        output_dir=STAGE1_OUT,
        group_num=GROUP_NUM,
        pick_per_group=PICK_PER_GROUP,
        car_length=CAR_LENGTH,
        car_width=CAR_WIDTH,
    )

    # ---------------------------------------------------
    # 第二阶段：以主体车为基准的时间裁剪
    #
    # 功能：
    # 1. 读取第一阶段生成的组合文件
    # 2. 依次将每一辆车视为主体车
    # 3. 保留时间范围完全覆盖主体车的其他车辆
    # 4. 丢弃不满足时间覆盖条件的车辆
    # 5. 检查剩余车辆数量是否达标
    # 6. 检查主体车时间跨度是否达标
    # 7. 将主体车编号固定为 0，其余车辆重新编号
    #
    # 输出：
    # 每个主体车生成一个子场景 JSON 文件
    # 文件按组合名存入 STAGE2_OUT 的子目录
    # ---------------------------------------------------
    stage2_subject_filter(
        input_dir=STAGE1_OUT,
        output_dir=STAGE2_OUT,
        min_vehicle_num=MIN_VEHICLE_NUM,
        min_duration=MIN_DURATION_STAGE2,
    )

    # ---------------------------------------------------
    # 第三阶段：最终场景筛选与汇总
    #
    # 功能：
    # 1. 递归遍历第二阶段生成的所有子场景文件
    # 2. 从文件名中解析主体车时长与轨迹数量
    # 3. 根据设定阈值进行筛选
    # 4. 将满足条件的文件复制到统一目录
    # 5. 通过父目录名避免文件名冲突
    #
    # 输出：
    # 所有满足条件的 JSON 文件
    # 统一存放在 FINAL_OUT 目录
    # ---------------------------------------------------
    stage3_collect(
        input_dir=STAGE2_OUT,
        output_dir=FINAL_OUT,
        min_duration=MIN_DURATION_STAGE3,
        min_traj_num=MIN_TRAJ_NUM,
    )


if __name__ == "__main__":
    main()
