/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.github.infolis.ws.client;


/**
 *
 * @author domi
 */
public class MyClient {

//    public static void main(String args[]) throws JSONException {
//
//        //Client c = Client.create();
//        Client c = ClientBuilder.newBuilder()
//        		.register(JacksonFeature.class)
//        		.register(JacksonJsonProvider.class)
//        		.build();
////        cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
////        cc.getClasses().add(JacksonJsonProvider.class);
////        Client c = Client.create(cc);
//        //WebResource r = c.resource("http://localhost:8080/TestWS/webresources/test");
//        WebTarget wResource = c.target("http://localhost:8080/TestWS/webresources/backend");
////        String response = r.accept(MediaType.TEXT_PLAIN).get(String.class);
////        System.out.println(response);
//        Execution e = new Execution();
//        ParameterValues i = new ParameterValues();
//        Map<String, Object> entry = new HashMap<>();
//        entry.put("infolis:algorithm", "PDF2Text");
//        InfolisFile in = new InfolisFile();
//        in.setFileId("in");
//        entry.put("pdfInput", in);
//        i.setValues(entry);
//        e.setInput(i);
//        wResource
//        	.request(MediaType.APPLICATION_JSON)
//        	.post(Entity.entity(e, MediaType.APPLICATION_JSON_TYPE));
////        wResource.accept().type(MediaType.APPLICATION_JSON_TYPE).post(Backend.class, e);
//        
//        
////        TestConfig inputJsonObj = new TestConfig();
////        inputJsonObj.setInput("C:\\Users\\domi\\InFoLiS2\\InfoLink\\PDFToText\\in");
////        TestConfig outputJsonObj = r.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON_TYPE).post(TestConfig.class, inputJsonObj);
////        System.out.println(outputJsonObj.getOutput());
//        
//        
//        
//        
//    }
}
