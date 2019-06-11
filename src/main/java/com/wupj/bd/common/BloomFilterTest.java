package com.wupj.bd.common;


import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * 〈布隆过滤器测试〉
 *
 * @author wupeiji
 * @date 2019/5/17 9:15
 * @since 1.0.0
 */
public class BloomFilterTest {

    public static void main(String[] args) {

        BloomFilter<Integer> bloomFilter = BloomFilter.create(Funnels.integerFunnel(), 500, 0.01);
        bloomFilter.put(1);
        bloomFilter.put(2);
        bloomFilter.put(3);
        bloomFilter.put(4);
        bloomFilter.put(5);
        final boolean exist = bloomFilter.mightContain(5);
        System.out.println("5 存在："+exist);

        System.out.println("6 存在："+bloomFilter.mightContain(6));
    }
}
