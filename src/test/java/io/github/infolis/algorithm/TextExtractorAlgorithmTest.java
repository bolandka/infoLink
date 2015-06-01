package io.github.infolis.algorithm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import io.github.infolis.algorithm.TextExtractorAlgorithm;
import io.github.infolis.datastore.DataStoreClient;
import io.github.infolis.datastore.DataStoreClientFactory;
import io.github.infolis.datastore.DataStoreStrategy;
import io.github.infolis.datastore.FileResolver;
import io.github.infolis.datastore.FileResolverFactory;
import io.github.infolis.datastore.TempFileResolver;
import io.github.infolis.model.Execution;
import io.github.infolis.model.ExecutionStatus;
import io.github.infolis.model.InfolisFile;
import io.github.infolis.util.SerializationUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextExtractorAlgorithmTest {

    Logger log = LoggerFactory.getLogger(TextExtractorAlgorithmTest.class);

    @Test
    public void testLocalFile() throws IOException {

        FileResolver resolver = new TempFileResolver();
        DataStoreClient client = DataStoreClientFactory.local();

        InfolisFile inFile = new InfolisFile();
        Execution execution = new Execution();

    //    Path tmpDir = Files.
        Path tempFile = Files.createTempFile("infolis-", ".pdf");
        String resPath = "/trivial.pdf";

        byte[] pdfBytes = IOUtils.toByteArray(getClass().getResourceAsStream(resPath));
        IOUtils.write(pdfBytes, Files.newOutputStream(tempFile));                
        
        inFile.setFileName(tempFile.toString());
        inFile.setMd5(SerializationUtils.getHexMd5(pdfBytes));
        inFile.setMediaType("application/pdf");
        inFile.setFileStatus("AVAILABLE");
        
        try {
        OutputStream os = resolver.openOutputStream(inFile);
        IOUtils.write(pdfBytes, os);
        os.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        client.post(InfolisFile.class, inFile);
        client.post(Execution.class, execution);
                
        System.out.println(inFile.getFileName());
        System.out.println(inFile.getUri());
        
        assertNotNull(inFile.getUri());

        execution.getInputFiles().add(inFile.getUri());
        execution.setAlgorithm(TextExtractorAlgorithm.class);

        assertEquals(1, execution.getInputFiles().size());
        assertEquals(inFile.getUri(), execution.getInputFiles().get(0));
        Algorithm algo = execution.instantiateAlgorithm(client, resolver);        
        algo.run();     

        log.debug("{}", execution.getOutputFiles());
        assertEquals(ExecutionStatus.FINISHED, algo.getExecution().getStatus());
        assertEquals(1, execution.getOutputFiles().size());

        
        
        String fileId = algo.getExecution().getOutputFiles().get(0);
        InfolisFile outFile = client.get(InfolisFile.class, fileId);
        InputStream in = resolver.openInputStream(outFile);
        String x = IOUtils.toString(in);
        in.close();
//		for (char c : x.toCharArray()) {
//            log.debug("{}", (int)c);
//		}
        assertEquals("Foo. Bar!", x.trim());
    }

}
