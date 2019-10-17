package org.elasticsearch.repositories.blobstore;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Repeat(iterations = 1000)
public class BlobStoreIndexInputTests extends ESTestCase {

    public void testReadByte() throws IOException {
        final byte[] data = {
            0, 0, 0, 0, 0,
            1, 1, 1, 1, 1,
            2, 2, 2, 2, 2,
            3, 3, 3, 3, 3,
            4, 4
        };

        final long partSize = 5L;
        final BlobStoreIndexShardSnapshot.FileInfo fileInfo = new BlobStoreIndexShardSnapshot.FileInfo("_blob",
            new StoreFileMetaData("_file", data.length, "_checksum", Version.LATEST), new ByteSizeValue(partSize));

        final BlobContainer container = mock(BlobContainer.class);
        when(container.path()).thenReturn(new BlobPath());
        doAnswer(invocationMock -> {
            int partNumber = Integer.parseInt(((String) invocationMock.getArguments()[0]).replace("_blob.part", ""));
            int offset = Math.toIntExact((partNumber * partSize) + (long) invocationMock.getArguments()[1]);
            int length = Math.toIntExact((long) invocationMock.getArguments()[2]);
            return new ByteArrayInputStream(Arrays.copyOfRange(data, offset, offset + length));
        }).when(container).readBlob(anyString(), anyLong(), anyLong());

        {
            final BlobStoreIndexInput blobStoreIndexInput = new BlobStoreIndexInput(container, fileInfo);

            assertThat(blobStoreIndexInput.length(), equalTo((long) data.length));
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(0L));

            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 0));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 0));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 0));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 0));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 0));

            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(5L));

            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 1));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 1));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 1));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 1));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 1));

            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(10L));

            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 2));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 2));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 2));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 2));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 2));

            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(15L));

            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 3));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 3));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 3));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 3));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 3));

            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(20L));

            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 4));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 4));

            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(22L));

            expectThrows(IOException.class, () -> blobStoreIndexInput.seek(-1L));

            blobStoreIndexInput.seek(10L);
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 2));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 2));
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(12L));

            blobStoreIndexInput.seek(20L);
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 4));
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(21L));

            blobStoreIndexInput.seek(5L);
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 1));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 1));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 1));
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(8L));

            blobStoreIndexInput.seek(15L);
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 3));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 3));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 3));
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(18L));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 3));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 3));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 4));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 4));

            blobStoreIndexInput.seek(0);
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(0L));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 0));

            blobStoreIndexInput.seek(data.length - 1);
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(21L));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) 4));
            assertThat(blobStoreIndexInput.readByte(), equalTo((byte) -1));

            blobStoreIndexInput.seek(data.length);
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo((long) data.length));

            expectThrows(EOFException.class, () -> blobStoreIndexInput.seek(data.length + 1));
        }
        {
            final BlobStoreIndexInput blobStoreIndexInput = new BlobStoreIndexInput(container, fileInfo);

            byte[] buffer = new byte[4];
            blobStoreIndexInput.readBytes(buffer, 0, buffer.length);
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(4L));
            assertArrayEquals(new byte[]{0, 0, 0, 0}, buffer);

            buffer = new byte[4];
            blobStoreIndexInput.readBytes(buffer, 0, buffer.length);
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(8L));
            assertArrayEquals(new byte[]{0, 1, 1, 1}, buffer);

            buffer = new byte[10];
            blobStoreIndexInput.readBytes(buffer, 0, buffer.length);
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(18L));
            assertArrayEquals(new byte[]{1, 1, 2, 2, 2, 2, 2, 3, 3, 3}, buffer);

            buffer = new byte[10];
            blobStoreIndexInput.readBytes(buffer, 0, buffer.length);
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(22L));
            assertArrayEquals(new byte[]{3, 3, 4, 4, 0, 0, 0, 0, 0, 0}, buffer);
        }
        {
            final BlobStoreIndexInput blobStoreIndexInput = new BlobStoreIndexInput(container, fileInfo);
            blobStoreIndexInput.seek(12L);

            byte[] buffer = new byte[9];
            blobStoreIndexInput.readBytes(buffer, 0, buffer.length);
            assertThat(blobStoreIndexInput.getFilePointer(), equalTo(21L));
            assertArrayEquals(new byte[]{2, 2, 2, 3, 3, 3, 3, 3, 4}, buffer);
        }
        {
            final BlobStoreIndexInput blobStoreIndexInput = new BlobStoreIndexInput(container, fileInfo);

            IndexInput slice = blobStoreIndexInput.slice("test", 10L, 5L);
            assertThat(slice.getFilePointer(), equalTo(0L));
            assertThat(slice.length(), equalTo(5L));

            byte[] buffer = new byte[(int) slice.length()];
            slice.readBytes(buffer, 0, buffer.length);
            assertThat(slice.getFilePointer(), equalTo(slice.length()));
            assertArrayEquals(new byte[]{2, 2, 2, 2, 2}, buffer);

            slice = blobStoreIndexInput.slice("test", 6L, 10L);
            assertThat(slice.getFilePointer(), equalTo(0L));
            assertThat(slice.length(), equalTo(10L));

            buffer = new byte[(int) slice.length()];
            slice.readBytes(buffer, 0, buffer.length);
            assertThat(slice.getFilePointer(), equalTo(slice.length()));
            assertArrayEquals(new byte[]{1, 1, 1, 1, 2, 2, 2, 2, 2, 3}, buffer);

            IndexInput subSlice = slice.slice("test", 3L, 2L);
            assertThat(subSlice.getFilePointer(), equalTo(0L));
            assertThat(subSlice.length(), equalTo(2L));

            buffer = new byte[(int) subSlice.length()];
            subSlice.readBytes(buffer, 0, buffer.length);
            assertThat(subSlice.getFilePointer(), equalTo(subSlice.length()));
            assertArrayEquals(new byte[]{1, 2}, buffer);
        }
    }

    public void testRandomReadsSinglePart() throws IOException {
        final byte[] data = randomByteArrayOfLength(randomIntBetween(1, 1000));

        final BlobStoreIndexShardSnapshot.FileInfo fileInfo = new BlobStoreIndexShardSnapshot.FileInfo("_blob",
            new StoreFileMetaData("_file", data.length, "_checksum", Version.LATEST), new ByteSizeValue(data.length));

        final BlobContainer container = mock(BlobContainer.class);
        when(container.path()).thenReturn(new BlobPath());
        doAnswer(invocationMock -> {
            if ("_blob".equals(invocationMock.getArguments()[0]) == false) {
                return new ByteArrayInputStream(new byte[0]);
            }
            int offset = Math.toIntExact((long) invocationMock.getArguments()[1]);
            int length = Math.toIntExact((long) invocationMock.getArguments()[2]);
            return new ByteArrayInputStream(Arrays.copyOfRange(data, offset, offset + length));
        }).when(container).readBlob(anyString(), anyLong(), anyLong());

        final BlobStoreIndexInput blobStoreIndexInput = new BlobStoreIndexInput(container, fileInfo);
        assertThat(blobStoreIndexInput.length(), equalTo((long) data.length));
        assertThat(blobStoreIndexInput.getFilePointer(), equalTo(0L));
        assertArrayEquals(data, randomReadAndSlice(blobStoreIndexInput, data.length));
    }

    public void testRandomReadsMultipleParts() throws IOException {
        final int partSize = randomIntBetween(1, 1000);
        final int parts = randomIntBetween(2, 10);

        final byte[] data;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            for (int i = 0; i < parts - 1; i++) {
                out.write(randomByteArrayOfLength(partSize));
            }
            out.write(randomByteArrayOfLength(partSize));
            out.flush();
            data = out.toByteArray();
        }

        final BlobStoreIndexShardSnapshot.FileInfo fileInfo = new BlobStoreIndexShardSnapshot.FileInfo("_blob",
            new StoreFileMetaData("_file", data.length, "_checksum", Version.LATEST), new ByteSizeValue(partSize));

        final BlobContainer container = mock(BlobContainer.class);
        when(container.path()).thenReturn(new BlobPath());
        doAnswer(invocationMock -> {
            int partNumber = Integer.parseInt(((String) invocationMock.getArguments()[0]).replace("_blob.part", ""));
            int offset = Math.toIntExact((partNumber * partSize) + (long) invocationMock.getArguments()[1]);
            int length = Math.toIntExact((long) invocationMock.getArguments()[2]);
            return new ByteArrayInputStream(Arrays.copyOfRange(data, offset, offset + length));
        }).when(container).readBlob(anyString(), anyLong(), anyLong());

        final BlobStoreIndexInput blobStoreIndexInput = new BlobStoreIndexInput(container, fileInfo);
        assertThat(blobStoreIndexInput.length(), equalTo((long) data.length));
        assertThat(blobStoreIndexInput.getFilePointer(), equalTo(0L));
        assertArrayEquals(data, randomReadAndSlice(blobStoreIndexInput, data.length));
    }

    private byte[] randomReadAndSlice(IndexInput indexInput, int length) throws IOException {
        int readPos = (int) indexInput.getFilePointer();
        byte[] output = new byte[length];
        while (readPos < length) {
            switch (randomIntBetween(0, 3)) {
                case 0:
                    // Read by one byte at a time
                    output[readPos++] = indexInput.readByte();
                    break;
                case 1:
                    // Read several bytes into target
                    int len = randomIntBetween(1, length - readPos);
                    indexInput.readBytes(output, readPos, len);
                    readPos += len;
                    break;
                case 2:
                    // Read several bytes into 0-offset target
                    len = randomIntBetween(1, length - readPos);
                    byte[] temp = new byte[len];
                    indexInput.readBytes(temp, 0, len);
                    System.arraycopy(temp, 0, output, readPos, len);
                    readPos += len;
                    break;
                case 3:
                    // Read using slice
                    len = randomIntBetween(1, length - readPos);
                    IndexInput slice = indexInput.slice("slice (" + readPos + ", " + len + ") of " + indexInput.toString(), readPos, len);
                    assertEquals(slice.length(), len);
                    temp = randomReadAndSlice(slice, len);
                    assertEquals(len, slice.getFilePointer());
                    System.arraycopy(temp, 0, output, readPos, len);
                    // assert that position in the original input didn't change
                    assertEquals(readPos, indexInput.getFilePointer());
                    readPos += len;
                    indexInput.seek(readPos);
                    assertEquals(readPos, indexInput.getFilePointer());
                    break;
                default:
                    fail();
            }
            assertEquals(readPos, indexInput.getFilePointer());
        }
        return output;
    }
}
