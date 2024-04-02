package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc 描述了一个元组的模式。
 */
public class TupleDesc implements Serializable {

    private ArrayList<TDItem> List;

    /**
     * 一个辅助类，用于组织每个字段的信息
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * 字段的类型
         */
        public final Type fieldType;
        
        /**
         * 字段的名称
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * 返回一个迭代器，它遍历包含在此TupleDesc中的所有字段TDItems
     */
    public Iterator<TDItem> iterator() {
        return List.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * 创建一个新的TupleDesc，具有typeAr.length个字段，这些字段具有指定的类型，与关联的命名字段。
     * 
     * @param typeAr 指定此TupleDesc中的字段数量和类型的数组。它必须至少包含一个条目。
     * @param fieldAr 指定字段名称的数组。注意，名称可以为null。
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        List = new ArrayList<TDItem>(typeAr.length);
        for (int i = 0; i < typeAr.length; i++) {
            List.set(i, new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * 构造函数。创建一个新的tuple desc，具有typeAr.length个字段，这些字段具有指定的类型，与匿名（未命名）字段。
     * 
     * @param typeAr 指定此TupleDesc中的字段数量和类型的数组。它必须至少包含一个条目。
     */
    public TupleDesc(Type[] typeAr) {
        List = new ArrayList<TDItem>(typeAr.length);
        for (int i = 0; i < typeAr.length; i++) {
            List.set(i, new TDItem(typeAr[i], null));
        }
    }

    /**
     * 返回此TupleDesc中的字段数量
     */
    public int numFields() {
        return List.size();
    }

    /**
     * 获取此TupleDesc的第i个字段的（可能为null的）字段名称。
     * 
     * @param i 要返回的字段名称的索引。它必须是一个有效的索引。
     * @return 第i个字段的名称
     * @throws NoSuchElementException 如果i不是一个有效的字段引用。
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < List.size()) {
            return List.get(i).fieldName;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * 获取此TupleDesc的第i个字段的类型。
     * 
     * @param i 要获取类型的字段的索引。它必须是一个有效的索引。
     * @return 第i个字段的类型
     * @throws NoSuchElementException 如果i不是一个有效的字段引用。
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < List.size()) {
            return List.get(i).fieldType;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * 查找具有给定名称的字段的索引。
     * 
     * @param name 字段的名称。
     * @return 首先具有给定名称的字段的索引。
     * @throws NoSuchElementException 如果找不到具有匹配名称的字段。
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < List.size(); ++i) {
            if (List.get(i).fieldName == name) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * 返回与此TupleDesc对应的元组的大小（以字节为单位）。
     * 注意，给定TupleDesc的元组是固定大小的。
     */
    public int getSize() {
        int result = 0;
        for (TDItem item : List) {
            result += item.fieldType.getLen();
        }
        return result;
    }

    /**
     * 将两个TupleDesc合并为一个，具有td1.numFields + td2.numFields字段，
     * 其中前td1.numFields字段来自td1，其余来自td2。
     * 
     * @param td1 具有新TupleDesc前几个字段的TupleDesc
     * @param td2 具有TupleDesc后几个字段的TupleDesc
     * @return 新的TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int len = td1.numFields() + td2.numFields();
        Type[] mergTypes = new Type[len];
        String[] mergStrings = new String[len];
        for (int i = 0; i < td1.numFields(); ++i) {
            mergTypes[i] = td1.getFieldType(i);
            mergStrings[i] = td1.getFieldName(i);
        }
        for (int i = 0; i < td2.numFields(); ++i) {
            mergTypes[td1.numFields() + i] = td2.getFieldType(i);
            mergStrings[td1.numFields() + i] = td2.getFieldName(i);
        }
        return new TupleDesc(mergTypes, mergStrings);
    }

    /**
     * 将指定对象与此TupleDesc进行比较以确定是否相等。如果它们的大小相同且
     * 此TupleDesc中的第n个类型等于td中的第n个类型，则认为两个TupleDescs相等。
     * 
     * @param o 与此TupleDesc比较等式的对象。
     * @return 如果对象等于此TupleDesc，则为true。
     */
    public boolean equals(Object o) {
        // 一些代码在这里
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleDesc tupleDesc = (TupleDesc) o;
        if (this.List.size() != tupleDesc.List.size()) return false;
        for (int i = 0; i < this.List.size(); i++) {
            Type type1 = this.List.get(i).fieldType;
            Type type2 = tupleDesc.List.get(i).fieldType;
            if (type1.ordinal() != type2.ordinal()) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // 如果您想将TupleDesc用作HashMap的键，请实现它，
        // 以便相等的对象具有相等的hashCode()结果
        int result = 1;
        for (TDItem item : this.List) {
            result = 31 * result + item.fieldType.ordinal();
        }
        return result;
        // throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * 返回描述此描述符的字符串。它应该是"fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])"的形式，
     * 尽管确切的格式并不重要。
     * 
     * @return 描述此描述符的字符串。
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < List.size(); i++) {
            TDItem item = List.get(i);
            if (i > 0) sb.append(", ");
            sb.append(item.fieldType.toString());
            if (item.fieldName != null && !item.fieldName.isEmpty()) {
                sb.append("(").append(item.fieldName).append(")");
            }
        }
        return sb.toString();
    }
}
