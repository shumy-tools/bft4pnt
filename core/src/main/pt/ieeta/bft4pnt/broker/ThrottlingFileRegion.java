package pt.ieeta.bft4pnt.broker;

import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

public class ThrottlingFileRegion extends AbstractReferenceCounted implements FileRegion {

  private static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultFileRegion.class);
  private final File f;
  private final long position;
  private final long time;
  private final long count;
  
  private long transferred;
  
  private FileChannel file;

  /**
   * Create a new instance
   *
   * @param file      the {@link FileChannel} which should be transferred
   * @param position  the position from which the transfer should start
   * @param count     the number of bytes to transfer
   */
  public ThrottlingFileRegion(FileChannel file, long position, long count) {
      if (file == null) {
          throw new NullPointerException("file");
      }
      checkPositiveOrZero(position, "position");
      checkPositiveOrZero(count, "count");
      this.file = file;
      this.position = position;
      this.time = System.currentTimeMillis();
      this.count = count;
      f = null;
  }

  /**
   * Create a new instance using the given {@link File}. The {@link File} will be opened lazily or
   * explicitly via {@link #open()}.
   *
   * @param f         the {@link File} which should be transferred
   * @param position  the position from which the transfer should start
   * @param count     the number of bytes to transfer
   */
  public ThrottlingFileRegion(File f, long position, long count) {
      if (f == null) {
          throw new NullPointerException("f");
      }
      checkPositiveOrZero(position, "position");
      checkPositiveOrZero(count, "count");
      this.position = position;
      this.time = System.currentTimeMillis();
      this.count = count;
      this.f = f;
  }

  /**
   * Returns {@code true} if the {@link FileRegion} has a open file-descriptor
   */
  public boolean isOpen() {
      return file != null;
  }

  /**
   * Explicitly open the underlying file-descriptor if not done yet.
   */
  public void open() throws IOException {
      if (!isOpen() && refCnt() > 0) {
          // Only open if this DefaultFileRegion was not released yet.
          file = new RandomAccessFile(f, "r").getChannel();
      }
  }

  @Override
  public long position() {
      return position;
  }

  @Override
  public long count() {
      return count;
  }

  @Deprecated
  @Override
  public long transfered() {
      return transferred;
  }

  @Override
  public long transferred() {
      return transferred;
  }

  @Override
  public long transferTo(WritableByteChannel target, long position) throws IOException {
      long count = this.count - position;
      if (count < 0 || position < 0) {
          throw new IllegalArgumentException("position out of range: " + position + " (expected: 0 - " + (this.count - 1) + ')');
      }
      
      if (count == 0) {
          return 0L;
      }
      
      if (refCnt() == 0) {
          throw new IllegalReferenceCountException(0);
      }
      
      // Call open to make sure fc is initialized. This is a no-oop if we called it before.
      open();
      
      long written = file.transferTo(this.position + position, count, target);
      if (written > 0) {
          transferred += written;
          
          // Throttling
          long speed = getSpeed();
          while (speed > 10000) {
            /*try { Thread.sleep(100); } catch (Exception e) {
              e.printStackTrace();
            }*/
            speed = getSpeed();
            //System.out.println("--Throttling--");
          }
          //System.out.println("WRITTEN: " + written + " of " + transferred + " (" + speed  + ")");
          
      } else if (written == 0) {
          validate(this, position);
      }
      
      return written;
  }

  //KB/s
  private long getSpeed() {
    long delta = (System.currentTimeMillis() - time);
    if (delta == 0)
      return 1000000;
    
    return transferred/delta;
  }
  
  @Override
  protected void deallocate() {
      FileChannel file = this.file;

      if (file == null) {
          return;
      }
      this.file = null;

      try {
          file.close();
      } catch (IOException e) {
          if (logger.isWarnEnabled()) {
              logger.warn("Failed to close a file.", e);
          }
      }
  }

  @Override
  public FileRegion retain() {
      super.retain();
      return this;
  }

  @Override
  public FileRegion retain(int increment) {
      super.retain(increment);
      return this;
  }

  @Override
  public FileRegion touch() {
      return this;
  }

  @Override
  public FileRegion touch(Object hint) {
      return this;
  }

  static void validate(ThrottlingFileRegion region, long position) throws IOException {
      // If the amount of written data is 0 we need to check if the requested count is bigger then the
      // actual file itself as it may have been truncated on disk.
      //
      // See https://github.com/netty/netty/issues/8868
      long size = region.file.size();
      long count = region.count - position;
      if (region.position + count + position > size) {
          throw new IOException("Underlying file size " + size + " smaller then requested count " + region.count);
      }
  }
}

