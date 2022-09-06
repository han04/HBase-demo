package com.relations;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.List;

public class DeleteRelationsProcessor extends BaseRegionObserver {
    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
                           Durability durability) throws IOException {
        // 监听表
        final HTable relations = (HTable)e.getEnvironment().getTable(TableName.valueOf("relations"));
        // 获取删除对象
        List<Cell> cells = delete.getFamilyCellMap().get(Bytes.toBytes("friends"));
        // 判断是否为空
        if (CollectionUtils.isEmpty(cells)) {
            relations.close();
            return;
        }
        // 循环删除
        for (int i = 0; i <cells.size() ; i++) {
            // 获取 uid1 第一个 column
            Cell cell = cells.get(0);

            // 创建 uid2， 并设置需要删除的 column
            Delete otherUserDelete = new Delete(CellUtil.cloneQualifier(cell));
            otherUserDelete.addColumns(Bytes.toBytes("friends"), CellUtil.cloneRow(cell));
            relations.delete(otherUserDelete);
        }
        // 关闭 table 对象
        relations.close();
    }
}
