package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Catalog 类负责跟踪数据库中所有可用表及其相关的模式。
 * 目前，这是一个必须由用户程序填充表格后才能使用的存根目录 —— 最终，这应该被转换为一个从磁盘读取目录表的目录。
 * 
 * @ThreadSafe
 */
public class Catalog {

    /**
     * 构造函数。
     * 创建一个新的，空的目录。
     */
    public Catalog() {
        // 这里填写一些代码
    }

    /**
     * 向目录中添加一个新表。
     * 此表的内容存储在指定的DbFile中。
     * 
     * @param file      要添加的表的内容；file.getId()是此文件/元组描述参数的标识符，用于getTupleDesc和getFile的调用
     * @param name      表的名称 —— 可能是一个空字符串。不得为null。如果存在名称冲突，使用最后添加的表作为给定名称的表。
     * @param pkeyField 主键字段的名称
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // 这里填写一些代码
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * 向目录中添加一个新表。
     * 此表的元组使用指定的TupleDesc格式化，并且其内容存储在指定的DbFile中。
     * 
     * @param file 要添加的表的内容；file.getId()是此文件/元组描述参数的标识符，用于getTupleDesc和getFile的调用
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * 返回具有指定名称的表的id，
     * 
     * @throws NoSuchElementException 如果表不存在
     */
    public int getTableId(String name) throws NoSuchElementException {
        // 这里填写一些代码
        return 0;
    }

    /**
     * 返回指定表的元组描述符（模式）
     * 
     * @param tableid 表的id，由addTable传递的DbFile.getId()函数指定
     * @throws NoSuchElementException 如果表不存在
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // 这里填写一些代码
        return null;
    }

    /**
     * 返回可以用来读取指定表内容的DbFile。
     * 
     * @param tableid 表的id，由addTable传递的DbFile.getId()函数指定
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // 这里填写一些代码
        return null;
    }

    public String getPrimaryKey(int tableid) {
        // 这里填写一些代码
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        // 这里填写一些代码
        return null;
    }

    public String getTableName(int id) {
        // 这里填写一些代码
        return null;
    }

    /** 从目录中删除所有表 */
    public void clear() {
        // 这里填写一些代码
    }

    /**
     * 从文件中读取模式并在数据库中创建相应的表。
     * 
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try (BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)))) {
            while ((line = br.readLine()) != null) {
                // assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                // System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
