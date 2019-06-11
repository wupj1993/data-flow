package com.wupj.bd.mr;

import lombok.*;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 〈自定义输出类型〉
 *
 * @author wupeiji
 * @date 2019/5/17 14:30
 * @since 1.0.0
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class UserMsg implements WritableComparable<UserMsg> {
    String name;
    String address;
    Integer age;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.name);
        out.writeUTF(this.address);
        out.writeInt(age);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.address = in.readUTF();
        this.age = in.readInt();
    }

    @Override
    public int compareTo(UserMsg o) {
        return o.age - this.age;
    }
}
