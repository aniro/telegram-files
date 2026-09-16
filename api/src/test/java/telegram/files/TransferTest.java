package telegram.files;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cn.hutool.core.io.FileUtil;
import io.vertx.core.json.JsonObject;
import telegram.files.Transfer.DuplicationPolicy;
import telegram.files.Transfer.TransferPolicy;
import telegram.files.repository.FileRecord;
import telegram.files.repository.SettingAutoRecords;

class TransferTest {
    private Transfer transfer;

    private FileRecord mockFileRecord;

    private Consumer<Transfer.TransferStatusUpdated> mockStatusUpdater;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        mockFileRecord = mock(FileRecord.class);
        mockStatusUpdater = mock(Consumer.class);

        SettingAutoRecords.TransferRule transferRule = new SettingAutoRecords.TransferRule();
        transferRule.destination = tempDir.toString();
        transferRule.transferPolicy = TransferPolicy.GROUP_BY_CHAT;
        transferRule.duplicationPolicy = DuplicationPolicy.OVERWRITE;
        transfer = Transfer.create(transferRule);
        transfer.transferStatusUpdated = mockStatusUpdater;
    }

    @Test
    void testCreateTransfer() {
        SettingAutoRecords.TransferRule groupByChatRule = new SettingAutoRecords.TransferRule();
        groupByChatRule.transferPolicy = TransferPolicy.GROUP_BY_CHAT;
        SettingAutoRecords.TransferRule groupByTypeRule = new SettingAutoRecords.TransferRule();
        groupByTypeRule.transferPolicy = TransferPolicy.GROUP_BY_TYPE;
        SettingAutoRecords.TransferRule groupByDateRule = new SettingAutoRecords.TransferRule();
        groupByDateRule.transferPolicy = TransferPolicy.GROUP_BY_DATE;
        Transfer chatTransfer = Transfer.create(groupByChatRule);
        Transfer typeTransfer = Transfer.create(groupByTypeRule);
        Transfer dateTransfer = Transfer.create(groupByDateRule);

        assertNotNull(chatTransfer);
        assertNotNull(typeTransfer);
        assertInstanceOf(Transfer.GroupByChat.class, chatTransfer);
        assertInstanceOf(Transfer.GroupByType.class, typeTransfer);
        assertInstanceOf(Transfer.GroupByDate.class, dateTransfer);
    }

    @Test
    void testGroupByDateUsesMessageDateAndConfiguredTimezone(@TempDir Path tempDir) {
        SettingAutoRecords.TransferRule transferRule = new SettingAutoRecords.TransferRule();
        transferRule.destination = tempDir.toString();
        transferRule.transferPolicy = TransferPolicy.GROUP_BY_DATE;
        transferRule.duplicationPolicy = DuplicationPolicy.RENAME;
        transferRule.extra = JsonObject.of(
                "timezone", "Asia/Shanghai",
                "dateGrouping", "YEAR_MONTH_DAY",
                "includeChatDirectory", true
        );
        when(mockFileRecord.localPath()).thenReturn(tempDir.resolve("photo.jpg").toString());
        when(mockFileRecord.chatId()).thenReturn(-100123L);
        when(mockFileRecord.date()).thenReturn(1788798600); // 2026-09-08 00:30 Asia/Shanghai

        String path = Transfer.create(transferRule).previewPath(mockFileRecord);

        assertEquals(tempDir.resolve("-100123").resolve("2026").resolve("09")
                .resolve("08").resolve("photo.jpg").toString(), path);
    }

    @Test
    void testCaptionNameSanitizesUnicodeAndLineBreaks(@TempDir Path tempDir) {
        SettingAutoRecords.TransferRule transferRule = new SettingAutoRecords.TransferRule();
        transferRule.destination = tempDir.toString();
        transferRule.transferPolicy = TransferPolicy.DIRECT;
        transferRule.useCaptionName = true;
        when(mockFileRecord.localPath()).thenReturn(tempDir.resolve("video.mp4").toString());
        when(mockFileRecord.caption()).thenReturn("Видос от #cappulait\n\n😳 Наши каналы | ФУЛЛ 👈");

        String path = Transfer.create(transferRule).previewPath(mockFileRecord);

        assertEquals(tempDir.resolve("video_Видос от #cappulait 😳 Наши каналы ФУЛЛ 👈.mp4").toString(), path);
    }

    @Test
    void testTransferSuccessful(@TempDir Path tempDir) {
        // Prepare mock file record
        String fileName = "source.txt";
        String sourcePath = mockWaitingTransfer(tempDir, fileName);

        // Transfer
        transfer.transfer(mockFileRecord);

        // Verify status updates and file moved
        verify(mockStatusUpdater, times(2)).accept(any());

        // Check final destination
        Path expectedDestination = Path.of(transfer.destination).resolve("456").resolve("789").resolve(fileName);
        assertTrue(Files.exists(expectedDestination));
        assertFalse(Files.exists(Paths.get(sourcePath)));
    }

    @Test
    void testDuplicationPolicySkip(@TempDir Path tempDir) {
        String fileName = "source.txt";

        // Prepare existing file
        createExistingFile(fileName);

        // Setup mock
        mockWaitingTransfer(tempDir, fileName);

        // Set skip policy
        transfer.duplicationPolicy = DuplicationPolicy.SKIP;

        // Transfer
        transfer.transfer(mockFileRecord);

        // Verify status updates
        verify(mockStatusUpdater, times(1)).accept(argThat(
                status -> status.transferStatus() == FileRecord.TransferStatus.idle
        ));
    }

    @Test
    void testDuplicationPolicyRename(@TempDir Path tempDir) {
        String fileName = "source.txt";
        // Prepare existing file
        String existingFile = createExistingFile(fileName);

        // Setup mock
        mockWaitingTransfer(tempDir, fileName);

        // Set rename policy
        transfer.duplicationPolicy = DuplicationPolicy.RENAME;

        // Transfer
        transfer.transfer(mockFileRecord);

        // Verify a new file was created with a suffix
        File[] filesInDir = FileUtil.ls(Path.of(existingFile).getParent().toString());
        assertTrue(filesInDir.length >= 2);
        assertTrue(Arrays.stream(filesInDir)
                .anyMatch(f -> f.getName().contains("source-1.txt")));
    }

    @Test
    void testTransferError(@TempDir Path tempDir) {
        // Force an error by providing an invalid path
        when(mockFileRecord.id()).thenReturn(123);
        when(mockFileRecord.localPath()).thenReturn("/invalid/nonexistent/path");

        // Transfer
        transfer.transfer(mockFileRecord);

        // Verify error status update
        verify(mockStatusUpdater, times(1)).accept(argThat(
                status -> status.transferStatus() == FileRecord.TransferStatus.error
        ));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "OPENAI_API_KEY", matches = ".+", disabledReason = "Requires OPENAI_API_KEY environment variable")
    void testGroupByAI_getTransferPath() {
        SettingAutoRecords.TransferRule transferRule = new SettingAutoRecords.TransferRule();
        transferRule.destination = "/tmp";
        transferRule.extra = JsonObject.of(
                "promptTemplate", """
                        Please categorize the following file into one of these categories: Documents, Images, Videos, Audio, Others.
                        File name: {file_name}
                        """
        );
        when(mockFileRecord.fileName()).thenReturn("As_Long_as_You_Love_Me-Backstreet_Boys-HQ.flac");
        Transfer.GroupByAI groupByAI = new Transfer.GroupByAI(transferRule);
        String path = groupByAI.getTransferPath(mockFileRecord);
        System.out.println("Obtained transfer path: " + path);
        assertNotNull(path);
    }

    private String mockWaitingTransfer(Path tempDir, String fileName) {
        String sourcePath = tempDir.resolve(fileName).toString();
        FileUtil.writeUtf8String("test content", sourcePath);

        when(mockFileRecord.id()).thenReturn(123);
        when(mockFileRecord.localPath()).thenReturn(sourcePath);
        when(mockFileRecord.telegramId()).thenReturn(456L);
        when(mockFileRecord.chatId()).thenReturn(789L);

        return sourcePath;
    }

    private String createExistingFile(String fileName) {
        Path existingFile = Path.of(transfer.destination).resolve("456").resolve("789").resolve(fileName);
        FileUtil.createTempFile(existingFile.getParent().toFile());
        FileUtil.writeUtf8String("existing content", existingFile.toString());

        return existingFile.toString();
    }

}
