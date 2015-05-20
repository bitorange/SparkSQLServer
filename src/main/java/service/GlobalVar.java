package service;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

/**
 * 全局系统变量
 */
public class GlobalVar {
    public static HashMap<String, String> configMap = new HashMap<String, String>();

    /**
     * 读取配置文件
     *
     * @throws org.apache.commons.configuration.ConfigurationException 读取 XML 配置文件失败
     */
    public static void readConFile() throws ConfigurationException {
        String filePath = "server.xml";
        XMLConfiguration config = new XMLConfiguration(filePath);
        NodeList list = config.getDocument().getElementsByTagName("entry");
        for (int i = 0; i < list.getLength(); i++) {
            Node node = list.item(i);
            String key = node.getAttributes().getNamedItem("key").getTextContent();
            String val = node.getTextContent();
            configMap.put(key, val);
        }
        System.out.println(configMap);
    }
}
