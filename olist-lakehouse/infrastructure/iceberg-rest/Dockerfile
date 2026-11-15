# Khai báo Image nền và phân quyền Root
FROM apache/iceberg-rest-fixture:latest

USER root

# Cài đặt thư viện kết nối PostgreSQL (JDBC Driver) cho phép khối Catalog Backend 
# bên trong REST Server mở kết nối mạng TCP/IP tới service catalog-postgres để thực thi 
# các câu lệnh SQL tra cứu và hoán đổi con trỏ nguyên tử (CAS).
RUN curl -fSL -o /usr/lib/iceberg-rest/postgresql.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Cài đặt thư viện giao tiếp MinIO/S3 (Iceberg AWS Bundle) cho phép khối FileIO trong REST
# Server gửi các lệnh đọc/ghi HTTP S3 sang bucket lakehouse trên MinIO.
RUN curl -fSL -o /usr/lib/iceberg-rest/iceberg-aws-bundle.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.0/iceberg-aws-bundle-1.5.0.jar

USER iceberg

# Định nghĩa lệnh khởi động dịch vụ Iceberg REST Server, chỉ định file JAR thực thi
# và các thư viện phụ thuộc (classpath) cần thiết để server giao tiếp với PostgreSQL
# và MinIO.
ENTRYPOINT ["java", "-cp", "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql.jar:/usr/lib/iceberg-rest/iceberg-aws-bundle.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
