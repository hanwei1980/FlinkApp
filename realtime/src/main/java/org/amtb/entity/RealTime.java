package org.amtb.entity;

import lombok.Data;
import org.nutz.dao.entity.annotation.Column;
import org.nutz.dao.entity.annotation.Name;
import org.nutz.dao.entity.annotation.Table;

/**
 * 实体bean
 * @author hanwei
 * @version 1.0
 * @date 2021-08-24 09:37
 */
@Data
@Table("REALTIME")
public class RealTime {
    @Column("UUID")
    @Name
    private String uuid;
    @Column("CREATE_TIME")
    private Long createTime;
    @Column("DEVICE_CODE")
    private String deviceCode;
    @Column("PRODUCTION")
    private int production;
    @Column("MACHINE_STATE")
    private String machineState;
    @Column("ACTIVE_ENERGY")
    private Double activeEnergy;
}
