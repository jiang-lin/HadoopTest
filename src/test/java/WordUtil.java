import com.jianglin.hadoop.Bean;
import org.apache.poi.POIXMLDocument;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xwpf.usermodel.*;
import org.apache.xmlbeans.XmlOptions;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTBody;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;

import static java.lang.System.in;

/**
 * Created by admin on 2017/11/27.
 */
public class WordUtil {
    public static void searchAndReplace(String srcPath, String destPath,
                                        Map<String, String> map,FileOutputStream fos ) {
        try {
            XWPFDocument document = new XWPFDocument(
                    POIXMLDocument.openPackage(srcPath));

//            XWPFDocument document = new XWPFDocument(
//                    new FileInputStream(srcPath));
            // 替换段落中的指定文字
            Iterator<XWPFParagraph> itPara = document.getParagraphsIterator();
            while (itPara.hasNext()) {
                XWPFParagraph paragraph =  itPara.next();
                //String s = paragraph.getParagraphText();
                Set<String> set = map.keySet();
                Iterator<String> iterator = set.iterator();
                while (iterator.hasNext()) {
                    String key = iterator.next();
                    List<XWPFRun> run=paragraph.getRuns();
                    for(XWPFRun xwpfRun:run){
                        if(xwpfRun.getText(xwpfRun.getTextPosition())!=null && xwpfRun.getText(xwpfRun.getTextPosition()).equals(key))
                        {
                            /**参数0表示生成的文字是要从哪一个地方开始放置,设置文字从位置0开始
                             * 就可以把原来的文字全部替换掉了
                             * */
                            xwpfRun.setText(map.get(key),0);
                        }
                    }
                    /*for(int i=0;i<run.size();i++)
                    {
                        if(run.get(i).getText(run.get(i).getTextPosition())!=null && run.get(i).getText(run.get(i).getTextPosition()).equals(key))
                        {
                            *//**参数0表示生成的文字是要从哪一个地方开始放置,设置文字从位置0开始
                             * 就可以把原来的文字全部替换掉了
                             * *//*
                            run.get(i).setText(map.get(key),0);
                        }
                    }*/
                }
            }

            // 替换表格中的指定文字
            Iterator<XWPFTable> itTable = document.getTablesIterator();
            while (itTable.hasNext()) {
                XWPFTable table = itTable.next();
                for (int i = 0; i < table.getNumberOfRows(); i++) {
                    XWPFTableRow row = table.getRow(i);
                    for (XWPFTableCell cell :  row.getTableCells()) {
                        for (Map.Entry<String, String> e : map.entrySet()) {
                            if (cell.getText().equals(e.getKey())) {
                                cell.removeParagraph(0);
                                cell.setText(e.getValue());
                            }
                        }
                    }
                }
            }



//            FileOutputStream outStream = null;
            String path="D:\\test\\aaa"+new Random().nextInt(9)+".doc";
            System.out.println(path);
            FileOutputStream outStream1  = new FileOutputStream(path);
            document.write(outStream1);





//                        outStream.write(document);
//            outStream.close();
//            OutputStream os = new FileOutputStream(destPath,true);
//            document.write(os);
//            close(os);
//            close(is);

           /* try {
                if(fis!=null)
                    fis.close();
                if(fos1!=null)
                    fos1.close();
            } catch (IOException e) {
                e.printStackTrace();
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void combineDoc() throws Exception {

        String outFilePath = "D:\\test\\";//filePath.substring(0,index);
        File filesPath = new File(outFilePath);
        File [] files = filesPath.listFiles();
        FileOutputStream os = new FileOutputStream(outFilePath + "接口文档.docx",true);
        FileInputStream inputStream=null;
        byte [] bytes = null;

        OPCPackage src1Package =null;
        XWPFDocument src1Document =null;
        for(File file : files){

            if(!file.getName().contains("template")){
                if(src1Package==null){
                    src1Package = OPCPackage.open(file.getPath());
                    src1Document = new XWPFDocument(src1Package);
                }else{
                    OPCPackage src2Package = OPCPackage.open(file.getPath());
                    CTBody src1Body = src1Document.getDocument().getBody();
                    XWPFDocument src2Document = new XWPFDocument(src2Package);
                    CTBody src2Body = src2Document.getDocument().getBody();
                    WordUtil.appendBody(src1Body, src2Body);

                }


            }

        }
        src1Document.write(os);
        os.close();

        /*XWPFDocument document = new XWPFDocument();
        FileOutputStream out = new FileOutputStream("D:\\simple.docx");
        document.write(out);*/




//        FileChannel mFileChannel = new FileOutputStream("D:\\test\\simple.pdf").getChannel();
//        FileChannel inFileChannel;

//        for(File fin : files){
////            File fin = new File(infile) ;
//            /*inFileChannel = new FileInputStream(fin).getChannel();
//            inFileChannel.transferTo(0, inFileChannel.size(),mFileChannel);
//            inFileChannel.close();*/
////            WordExtractor extractor = new WordExtractor();
////            String str = extractor.extractText(in);
//
//
//        }
//        mFileChannel.close();



    }

    private static void appendBody(CTBody src, CTBody append) throws Exception {
        XmlOptions optionsOuter = new XmlOptions();
        optionsOuter.setSaveOuter();
        String appendString = append.xmlText(optionsOuter);
        String srcString = src.xmlText();
        String prefix = srcString.substring(0,srcString.indexOf(">")+1);
        String mainPart = srcString.substring(srcString.indexOf(">")+1,srcString.lastIndexOf("<"));
        String sufix = srcString.substring( srcString.lastIndexOf("<") );
        String addPart = appendString.substring(appendString.indexOf(">") + 1, appendString.lastIndexOf("<"));
        CTBody makeBody = CTBody.Factory.parse(prefix+mainPart+addPart+sufix);
        src.set(makeBody);
    }
    /**
     * 关闭输入流
     *
     * @param is
     */
    private static void close(InputStream is) {
        if (is != null) {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭输出流
     *
     * @param os
     */
    private static void close(OutputStream os) {
        if (os != null) {
            try {
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        /*Map<String, String> map = new HashMap<String, String>();
        map.put("${interfaceName}", "dddddddddddddddddddddd");
        map.put("${moudleCode}", "8886666");
        map.put("${funcCode}", "1243411");
        String srcPath = "D:\\template.docx";
        String destPath = "D:\\bbbbbbbbbbbbbb.docx";
        FileOutputStream outStream  = new FileOutputStream(destPath,true);
        searchAndReplace(srcPath, destPath, map,null);
        System.out.println("1111");
        searchAndReplace(srcPath, destPath, map,null);*/
       // combineDoc();
        convertToXml(new Bean(),"UTF-8");


    }


    public static String convertToXml(Object obj, String encoding) {
        String result = null;
        try {
            //Object aa=obj.newInstance();
            JAXBContext context = JAXBContext.newInstance(obj.getClass());
            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            marshaller.setProperty(Marshaller.JAXB_ENCODING, encoding);

            StringWriter writer = new StringWriter();
            marshaller.marshal(obj, writer);
            result = writer.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }
}
