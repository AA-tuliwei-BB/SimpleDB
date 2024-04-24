package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Tuple 维护了一个元组内容的信息。元组有一个由 TupleDesc 对象指定的指定模式，并包含带有每个字段数据的 Field 对象。
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private TupleDesc td;
    private RecordId rid;
    private ArrayList<Field> fields;

    /**
     * 根据指定的模式（类型）创建一个新的元组。
     *
     * @param td
     *            这个元组的模式。它必须是一个有效的 TupleDesc 实例，且至少包含一个字段。
     */
    public Tuple(TupleDesc td) {
        this.td = td;
        fields = new ArrayList<Field>(td.numFields());
        for (int i = 0; i < td.numFields(); ++i) {
            fields.add(null);
        }
    }

    /**
     * @return 表示这个元组模式的 TupleDesc。
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * @return 表示这个元组在磁盘上位置的 RecordId。可能为 null。
     */
    public RecordId getRecordId() {
        return rid;
    }

    /**
     * 为这个元组设置 RecordId 信息。
     *
     * @param rid
     *            这个元组的新 RecordId。
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * 改变这个元组的第 i 个字段的值。
     *
     * @param i
     *            要更改的字段的索引。必须是一个有效的索引。
     * @param f
     *            字段的新值。
     */
    public void setField(int i, Field f) {
        fields.set(i, f);
    }

    /**
     * @return 第 i 个字段的值，如果它还没有被设置，则返回 null。
     *
     * @param i
     *            要返回的字段索引。必须是一个有效的索引。
     */
    public Field getField(int i) {
        return fields.get(i);
    }

    /**
     * 以字符串形式返回这个 Tuple 的内容。注意，为了通过系统测试，格式需要如下所示：
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * 其中 \t 是任何空白符（换行符除外）
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (i > 0) sb.append(" ");
            sb.append(field.toString());
        }
        return sb.toString();
        // throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        一个迭代器，它迭代这个元组的所有字段
     * */
    public Iterator<Field> fields()
    {
        return fields.iterator();
    }

    /**
     * 重置这个元组的 TupleDesc
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.td = td;
        fields = new ArrayList<Field>(td.numFields());
    }
}
