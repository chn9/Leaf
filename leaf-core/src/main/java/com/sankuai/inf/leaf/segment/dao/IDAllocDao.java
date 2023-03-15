package com.sankuai.inf.leaf.segment.dao;

import com.sankuai.inf.leaf.segment.model.LeafAlloc;

import java.util.List;

public interface IDAllocDao {
    /**
     * 获取所有的业务key对应的发好配置
     *
     * @return 所有的业务key对应的发好配置
     */
    List<LeafAlloc> getAllLeafAllocs();

    /**
     * 更新数据库的最大id值，并返回LeafAlloc
     *
     * @param tag 业务key
     * @return 更新数据库的最大id值，并返回LeafAlloc
     */
    LeafAlloc updateMaxIdAndGetLeafAlloc(String tag);

    /**
     * 依据动态调整的step值，更新DB的最大id值，并返回更新后的记录
     *
     * @param leafAlloc leafAlloc
     * @return 依据动态调整的step值，更新DB的最大id值，并返回更新后的记录
     */
    LeafAlloc updateMaxIdByCustomStepAndGetLeafAlloc(LeafAlloc leafAlloc);

    /**
     * 查询所有的业务key（biz_tag）
     *
     * @return 所有的业务key（biz_tag）
     */
    List<String> getAllTags();
}
