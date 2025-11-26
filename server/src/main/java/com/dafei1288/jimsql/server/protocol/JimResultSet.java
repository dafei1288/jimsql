package com.dafei1288.jimsql.server.protocol;

import java.util.List;
import java.util.ArrayList;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class JimResultSet {
    private final List<String> columnNames;
    private final List<String> columnTypes;
    private final List<List<Object>> rows;
    
    public JimResultSet() {
        this.columnNames = new ArrayList<>();
        this.columnTypes = new ArrayList<>();
        this.rows = new ArrayList<>();
    }
    
    public void addColumn(String name, String type) {
        columnNames.add(name);
        columnTypes.add(type);
    }
    
    public void addRow(List<Object> row) {
        if (row.size() != columnNames.size()) {
            throw new IllegalArgumentException("Row size does not match column count");
        }
        rows.add(row);
    }
    
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // Write column count
        dos.writeInt(columnNames.size());
        
        // Write column metadata
        for (int i = 0; i < columnNames.size(); i++) {
            dos.writeUTF(columnNames.get(i));
            dos.writeUTF(columnTypes.get(i));
        }
        
        // Write row count
        dos.writeInt(rows.size());
        
        // Write rows
        for (List<Object> row : rows) {
            for (Object value : row) {
                if (value == null) {
                    dos.writeByte(0);
                } else if (value instanceof String) {
                    dos.writeByte(1);
                    dos.writeUTF((String) value);
                } else if (value instanceof Integer) {
                    dos.writeByte(2);
                    dos.writeInt((Integer) value);
                } else if (value instanceof Long) {
                    dos.writeByte(3);
                    dos.writeLong((Long) value);
                } else if (value instanceof Double) {
                    dos.writeByte(4);
                    dos.writeDouble((Double) value);
                } else if (value instanceof Boolean) {
                    dos.writeByte(5);
                    dos.writeBoolean((Boolean) value);
                } else {
                    throw new IllegalArgumentException("Unsupported data type: " + value.getClass());
                }
            }
        }
        
        return baos.toByteArray();
    }
    
    public static JimResultSet deserialize(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        JimResultSet rs = new JimResultSet();
        
        // Read column count
        int columnCount = dis.readInt();
        
        // Read column metadata
        for (int i = 0; i < columnCount; i++) {
            String name = dis.readUTF();
            String type = dis.readUTF();
            rs.addColumn(name, type);
        }
        
        // Read row count
        int rowCount = dis.readInt();
        
        // Read rows
        for (int i = 0; i < rowCount; i++) {
            List<Object> row = new ArrayList<>();
            for (int j = 0; j < columnCount; j++) {
                byte type = dis.readByte();
                switch (type) {
                    case 0:
                        row.add(null);
                        break;
                    case 1:
                        row.add(dis.readUTF());
                        break;
                    case 2:
                        row.add(dis.readInt());
                        break;
                    case 3:
                        row.add(dis.readLong());
                        break;
                    case 4:
                        row.add(dis.readDouble());
                        break;
                    case 5:
                        row.add(dis.readBoolean());
                        break;
                    default:
                        throw new IOException("Unknown data type: " + type);
                }
            }
            rs.addRow(row);
        }
        
        return rs;
    }
    
    public List<String> getColumnNames() {
        return columnNames;
    }
    
    public List<String> getColumnTypes() {
        return columnTypes;
    }
    
    public List<List<Object>> getRows() {
        return rows;
    }
} 