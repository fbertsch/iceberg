package com.netflix.iceberg.security;

import com.netflix.iceberg.metacat.S3ClientSupplier;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;

public class MixedFileIO implements FileIO, Closeable, HadoopConfigurable {

  private transient volatile HadoopFileIO hadoopIO;
  private transient volatile ConcurrentHashMap<String, S3FileIO> secureIOs;
  private SerializableSupplier<Configuration> hadoopConf;
  private final S3ClientSupplier s3ClientSupplier;
  private final S3FileIOProperties s3FileIOProperties;

  // Changed this constructor for 1.4.x rebase to use the newer s3fileioproperties convention
  public MixedFileIO(Configuration hadoopConf, S3ClientSupplier s3ClientSupplier, S3FileIOProperties s3fileIoProperties) {
    this.hadoopConf = new SerializableConfiguration(hadoopConf)::get;
    this.s3ClientSupplier = s3ClientSupplier;
    this.s3FileIOProperties = s3fileIoProperties;
  }

  @Override
  public InputFile newInputFile(String path) {
    return selectIO(path).newInputFile(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return selectIO(path).newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    selectIO(path).deleteFile(path);
  }

  @Override
  public void close() {
    getSecureIOs().values().forEach(s3FileIO -> s3FileIO.close());
    if (Closeable.class.isInstance(getHadoopIO())) {
      try {
        Closeable.class.cast(getHadoopIO()).close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private FileIO selectIO(String path) {
    String bucket = SecurityUtil.extractS3Bucket(path);
    if (isSecureBucket(bucket)) {
      return getSecureIO(getSecureBucketRegion(bucket));
    } else {
      return getHadoopIO();
    }
  }

  private boolean isSecureBucket(String bucket) {
    return SecurityUtil.SECURE_BUCKETS_FOR_REGIONS.containsKey(bucket);
  }

  private String getSecureBucketRegion(String bucket) {
    return SecurityUtil.SECURE_BUCKETS_FOR_REGIONS.get(bucket);
  }

  // This has to be created on demand otherwise it will null after serialization / serialization
  private FileIO getHadoopIO() {
    if (hadoopIO == null) {
      synchronized (this) {
        if (hadoopIO == null) {
          hadoopIO = new HadoopFileIO(hadoopConf.get());
        }
      }
    }
    return hadoopIO;
  }

  // This has to be created on demand otherwise it will null after serialization / serialization
  private ConcurrentHashMap<String, S3FileIO> getSecureIOs() {
    if (secureIOs == null) {
      synchronized (this) {
        if (secureIOs == null) {
          secureIOs = new ConcurrentHashMap<>();
        }
      }
    }
    return secureIOs;
  }

  private FileIO getSecureIO(String region) {
    return getSecureIOs().computeIfAbsent(region, key -> {
      S3ClientSupplier supplierForRegion = this.s3ClientSupplier.cloneWithRegion(region);
      supplierForRegion.init(hadoopConf.get());
      return new S3FileIO(supplierForRegion, s3FileIOProperties);
    });
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = new SerializableConfiguration(conf)::get;
  }

  @Override
  public Configuration getConf() {
    return hadoopConf.get();
  }

  @Override
  public void serializeConfWith(Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    this.hadoopConf = confSerializer.apply(getConf());
  }
}