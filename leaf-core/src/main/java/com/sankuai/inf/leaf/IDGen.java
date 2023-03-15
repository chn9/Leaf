package com.sankuai.inf.leaf;

import com.sankuai.inf.leaf.common.Result;

public interface IDGen {
    /**
     * 获取指定key的下一个id
     *
     * @param key key
     * @return 指定key的下一个id
     */
    Result get(String key);

    /**
     * 初始化
     *
     * @return 是否初始化成功，true-是，false-否
     */
    boolean init();
}
