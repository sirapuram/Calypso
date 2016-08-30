package com.citigroup.project.gui.framework;

import org.jdom.Document;
import org.jdom.input.SAXBuilder;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionListener;
import java.io.*;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.*;
import java.util.List;
import com.calypso.tk.core.Log;

/**
 * The XMLSwingRenderer class is the engine able to convert an XML descriptor
 * into a java.swing UI.
 */
public class XMLSwingRenderer {

    private static final String XML_ERROR = "Invalid XML Descriptor.";
    private static final String IO_ERROR = "Resource could not be found ";
    private static final String MAPPING_ERROR = " Resource type could not be mapped.";
    private static Frame appFrame;
    private static String default_resource_bundle_name = null;
    private static Locale default_locale = Locale.getDefault();
    private Parser parser = new Parser(this);
    private Object client;
    private Container root;
    private Map idmap = new HashMap();
    private Collection components = null;
    private Localizer localizer = new Localizer();
    private final TagLibrary taglib = SwingTagLibrary.getInstance();
    protected ClassLoader cl = this.getClass().getClassLoader();
    
    private static final String CLASS_NAME = "XMLSwingRenderer";
    
    static {
        System.out.println("UI XML");
    }

    public XMLSwingRenderer() {
        this.client = this;
        this.setLocale(XMLSwingRenderer.default_locale);
        this.getLocalizer().setResourceBundle(
                XMLSwingRenderer.default_resource_bundle_name);
    }

    /**
     * Constructor to be used if the XMLSwingRenderer is used by object
     * composition.
     */
    public XMLSwingRenderer(Object client) {
        this();
        this.client = client;
    }

    /**
     * Constructs a new XMLSwingRenderer, rendering the provided XML into a
     * javax.swing UI
     */
    public XMLSwingRenderer(final String resource) {
        this(XMLSwingRenderer.class.getClassLoader(), resource);
    }

    private XMLSwingRenderer(ClassLoader cl, final String resource) {
        this();
        this.setClassLoader(cl);
        Reader reader = null;
        try {
            InputStream in = cl.getResourceAsStream(resource);
            if (in == null) {
                throw new IOException(IO_ERROR + resource);
            }
            reader = new InputStreamReader(in);
            render(reader);
        }
        catch (Exception e) {
            Log.error(CLASS_NAME, e.getMessage());
        }
        finally {
            try {
                if(reader!=null)
                    reader.close();
            }
            catch (Exception e) {
                Log.warn(CLASS_NAME, e.getMessage());
            }
        }
    }

    /**
     * Gets the parsing of the XML started.
     * 
     * @param url <code>URL</code> url pointing to an XML descriptor
     * @return <code>Object</code>- instanced swing object tree root
     * @throws Exception
     */
    public Container render(final URL url) throws Exception {
        Reader reader = null;
        Container obj = null;
        try {
            InputStream in = url.openStream();
            if (in == null) {
                throw new IOException(IO_ERROR + url.toString());
            }
            reader = new InputStreamReader(in);
            obj = render(reader);
        }
        finally {
            try {
                if(reader !=null)
                    reader.close();
            }
            catch (Exception ex) {
                Log.warn(CLASS_NAME, ex.getMessage());
            }
        }
        return obj;
    }

    /**
     * Gets the parsing of the XML file started.
     * @param resource <code>String</code> xml-file path info
     * @return <code>Object</code>- instanced swing object tree root
     */
    public Container render(final String resource) throws Exception {
        Reader reader = null;
        Container obj = null;
        try {
            InputStream in = cl.getResourceAsStream(resource);
            if (in == null) {
                throw new IOException(IO_ERROR + resource);
            }
            reader = new InputStreamReader(in);
            obj = render(reader);
        }
        finally {
            try {
                if(reader !=null)
                    reader.close();
            }
            catch (Exception ex) {
                Log.warn(CLASS_NAME, ex.getMessage());
            }
        }
        return obj;
    }

    /**
     * Gets the parsing of the XML file started.
     * @param xml_file <code>File</code> xml-file
     * @return <code>Object</code>- instanced swing object tree root
     */
    public Container render(final File xml_file) throws Exception {
        if (xml_file == null) {
            throw new IOException();
        }
        return render(new FileReader(xml_file));
    }

    /**
     * Gets the parsing of the XML file started.
     * @param xml_reader <code>Reader</code> xml-file path info
     * @return <code>Object</code>- instanced swing object tree root
     */
    public Container render(final Reader xml_reader) throws Exception {
        if (xml_reader == null) {
            throw new IOException();
        }
        try {
            return render(new SAXBuilder().build(xml_reader));
        }
        catch (org.xml.sax.SAXParseException e) {
            Log.error(CLASS_NAME, e.getMessage());
        }
        catch (org.jdom.input.JDOMParseException e) {
            Log.error(CLASS_NAME, e.getMessage());
        }
        throw new Exception(XMLSwingRenderer.XML_ERROR);
    }

    /**
     * Gets the parsing of the XML file started.
     */
    public Container render(final Document jdoc) throws Exception {
        idmap.clear();
        try {
            root = (Container) parser.parse(jdoc);
        }
        catch (Exception e) {
            Log.error(CLASS_NAME, e.getMessage());
            throw (e);
        }
        components = null;
        mapMembers(client);
        if (Frame.class.isAssignableFrom(root.getClass())) {
            XMLSwingRenderer.setAppFrame((Frame) root);
        }
        return root;
    }

    /**
     * Inserts swing object rendered from an XML document into the given
     * container. <p/>
     * <pre>
     *   Differently to the render methods, insert does NOT consider the root node of the XML document.
     * </pre>
     * <pre>
     *   &lt;b&gt;NOTE:&lt;/b&gt;
     * <br>
     * insert() does NOT clear() the idmap before rendering. Therefore, if this XMLSwingRenderer's parser was used before, the idmap still contains (key/value) pairs (id, JComponent obj. references).
     * <br>
     * If insert() is NOT used to insert in a previously (with this very XMLSwingRenderer) rendered UI, it is recommended that you clear the idmap:
     *   &lt;div&gt;
     * <code>
     * mySwingEngine.getIdMap().clear()
     * </code>
     *   &lt;/div&gt;
     * </pre>
     */
    public void insert(final URL url, final Container container)
            throws Exception {
        Reader reader = null;
        try {
            InputStream in = url.openStream();
            if (in == null) {
                throw new IOException(IO_ERROR + url.toString());
            }
            reader = new InputStreamReader(in);
            insert(reader, container);
        }
        finally {
            try {
                if(reader !=null)
                    reader.close();
            }
            catch (Exception ex) {
                Log.warn(CLASS_NAME, ex.getMessage());
            }
        }
    }

    /**
     * Inserts swing objects rendered from an XML reader into the given
     * container. <p/>
     */
    public void insert(final Reader reader, final Container container)
            throws Exception {
        if (reader == null) {
            throw new IOException();
        }
        insert(new SAXBuilder().build(reader), container);
    }

    /**
     * Inserts swing objects rendered from an XML reader into the given
     * container. <p/>
     */
    public void insert(final String resource, final Container container)
            throws Exception {
        Reader reader = null;
        try {
            InputStream in = cl.getResourceAsStream(resource);
            if (in == null) {
                throw new IOException(IO_ERROR + resource);
            }
            reader = new InputStreamReader(in);
            insert(reader, container);
        }
        finally {
            try {
                if(reader !=null)
                    reader.close();
            }
            catch (Exception ex) {
                Log.warn(CLASS_NAME, ex.getMessage());
            }
        }
    }

    /**
     * Inserts swing objects rendered from an XML document into the given
     * container. <p/>
     * <pre>
     *   Differently to the parse methods, insert does NOT consider the root node of the XML document.
     * </pre>
     */
    public void insert(final Document jdoc, final Container container)
            throws Exception {
        root = container;
        try {
            parser.parse(jdoc, container);
        }
        catch (Exception e) {
            Log.error(CLASS_NAME, e.getMessage());
            throw (e);
        }
        components = null;
        mapMembers(client);
    }

    /**
     * Sets the XMLSwingRenderer's global resource bundle name, to be used by
     * all XMLSwingRenderer instances. This name can be overwritten however for
     * a single instance, if a <code>bundle</code> attribute is places in the
     * root tag of an XML descriptor.
     * 
     * @param bundlename <code>String</code> the resource bundle name.
     */
    public static void setResourceBundleName(String bundlename) {
        XMLSwingRenderer.default_resource_bundle_name = bundlename;
    }

    /**
     * Sets the XMLSwingRenderer's global locale, to be used by all
     * XMLSwingRenderer instances. This locale can be overwritten however for a
     * single instance, if a <code>locale</code> attribute is places in the
     * root tag of an XML descriptor.
     */
    public static void setDefaultLocale(Locale locale) {
        XMLSwingRenderer.default_locale = locale;
    }

    /**
     * Sets the XMLSwingRenderer's global application frame variable, to be used
     * as a parent for all child dialogs.
     */
    public static void setAppFrame(Frame frame) {
        if (frame != null) {
            if (XMLSwingRenderer.appFrame == null) {
                XMLSwingRenderer.appFrame = frame;
            }
        }
    }

    /**
     * @return <code>Frame</code> a parent for all dialogs.
     */
    public static Frame getAppFrame() {
        return XMLSwingRenderer.appFrame;
    }

    /**
     * Returns the object which instantiated this XMLSwingRenderer.
     * @return <code>Objecy</code> XMLSwingRenderer client object <p/>
     * <pre>
     * &lt;b&gt;Note:&lt;/b&gt;
     * <br>
     * This is the object used through introspection the actions and fileds are set.
     * </pre>
     */
    public Object getClient() {
        return client;
    }

    /**
     * Returns the root component of the generated Swing UI.
     * @return <code>Component</code>- the root component of the javax.swing
     *         ui
     */
    public Container getRootComponent() {
        return root;
    }

    /**
     * Returns an Iterator for all parsed GUI components.
     * @return <code>Iterator</code> GUI components itearator
     */
    public Iterator getAllComponentItertor() {
        if (components == null) {
            traverse(root, components = new ArrayList());
        }
        return components.iterator();
    }

    /**
     * Returns an Iterator for id-ed parsed GUI components.
     * 
     * @return <code>Iterator</code> GUI components itearator
     */
    public Iterator getIdComponentItertor() {
        return idmap.values().iterator();
    }

    /**
     * Returns the id map, containing all id-ed parsed GUI components.
     * 
     * @return <code>Map</code> GUI components map
     */
    public Map getIdMap() {
        return idmap;
    }

    /**
     * Removes all un-displayable components from the id map and deletes the
     * components collection. <br/>
     * Reccreated at the next request. <p/>
     * <pre>
     *   A component is made undisplayable either when it is removed from a displayable containment hierarchy or when its containment hierarchy is made undisplayable. A containment hierarchy is made undisplayable when its ancestor window is disposed.
     * </pre>
     */
    public int cleanup() {
        List zombies = new ArrayList();
        Iterator it = idmap.keySet().iterator();
        while (it != null && it.hasNext()) {
            Object key = it.next();
            Object obj = idmap.get(key);
            if (obj instanceof Component && !((Component) obj).isDisplayable()) {
                zombies.add(key);
            }
        }
        for (int i = 0; i < zombies.size(); i++) {
            idmap.remove(zombies.get(i));
        }
        components = null;
        return zombies.size();
    }

    /**
     * Removes the id from the internal from the id map, to make the given id
     * available for re-use.
     * @param id <code>String</code> assigned name
     */
    public void forget(final String id) {
        idmap.remove(id);
    }

    /**
     * Returns the GUI component with the given name or null.
     * @param id <code>String</code> assigned name
     * @return <code>Component</code>- the GUI component with the given name or null if not found.
     */
    public Component find(final String id) {
        return (Component) idmap.get(id);
    }

    /**
     * Sets the locale to be used during parsing / String conversion
     * @param l <code>Locale</code>
     */
    public void setLocale(Locale l) {
        this.localizer.setLocale(l);
    }

    /**
     * Sets the ResourceBundle to be used during parsing / String conversion
     * @param bundlename <code>String</code>
     */
    public void setResourceBundle(String bundlename) {
        this.localizer.setResourceBundle(bundlename);
    }

    public Localizer getLocalizer() {
        return localizer;
    }

    public TagLibrary getTaglib() {
        return taglib;
    }

    public void setClassLoader(ClassLoader cl) {
        this.cl = cl;
        this.localizer.setClassLoader(cl);
    }

    public ClassLoader getClassLoader() {
        return cl;
    }

    /**
     * Recursively Sets an ActionListener
     * @param c <code>Component</code> start component
     * @param al <code>ActionListener</code>
     */
    public boolean setActionListener(final Component c, final ActionListener al) {
        boolean b = false;
        if (c != null) {
            if (Container.class.isAssignableFrom(c.getClass())) {
                final Component[] s = ((Container) c).getComponents();
                for (int i = 0; i < s.length; i++) {
                    b = b | setActionListener(s[i], al);
                }
            }
            if (!b) {
                if (JMenu.class.isAssignableFrom(c.getClass())) {
                    final JMenu m = (JMenu) c;
                    final int k = m.getItemCount();
                    for (int i = 0; i < k; i++) {
                        b = b | setActionListener(m.getItem(i), al);
                    }
                }
                else if (AbstractButton.class.isAssignableFrom(c.getClass())) {
                    ((AbstractButton) c).addActionListener(al);
                    b = true;
                }
            }

        }
        return b;
    }

    public Iterator getDescendants(final Component c) {
        List list = new ArrayList(12);
        XMLSwingRenderer.traverse(c, list);
        return list.iterator();
    }

    /**
     * Introspects the given object's class and initializes its public fields
     * with objects that have been instanced during parsing. Mappping happens
     * based on type and field name: the fields name has to be equal to the tag
     * id, psecified in the XML descriptor. The fields class has to be
     * assignable (equals or super class..) from the class that was used to
     * instance the tag.
     * 
     * @param obj
     *            <code>Object</code> target object to be mapped with
     *            instanced tags
     */
    protected void mapMembers(Object obj) {
        Field[] flds = obj.getClass().getFields();
        int n = flds.length;
        for (int i = 0; i < n; i++) {
            Object uiobj = idmap.get(flds[i].getName());
            if (uiobj != null) {
                if (flds[i].getType().isAssignableFrom(uiobj.getClass())) {
                    try {
                        flds[i].set(obj, uiobj);
                    }
                    catch (IllegalArgumentException e) {
                        Log.warn(CLASS_NAME, e.getMessage());
                    }
                    catch (IllegalAccessException e) {
                        Log.warn(CLASS_NAME, e.getMessage());
                    }
                }
            }
            if (flds[i] == null) {
                try {
                    flds[i].set(obj, flds[i].getType().newInstance());
                }
                catch (IllegalArgumentException e) {
                    Log.warn(CLASS_NAME, e.getMessage());
                }
                catch (IllegalAccessException e) {
                    Log.warn(CLASS_NAME, e.getMessage());
                }
                catch (InstantiationException e) {
                    Log.warn(CLASS_NAME, e.getMessage());                }
            }
            if (flds[i] == null) {
                Log.error(CLASS_NAME, flds[i].getType() + " : "  + flds[i].getName() + XMLSwingRenderer.MAPPING_ERROR);
            }
        }
    }

    protected static void traverse(final Component c, Collection collection) {
        if (c != null) {
            collection.add(c);
            if (c instanceof JMenu) {
                final JMenu m = (JMenu) c;
                final int k = m.getItemCount();
                for (int i = 0; i < k; i++) {
                    traverse(m.getItem(i), collection);
                }
            }
            else if (c instanceof Container) {
                final Component[] s = ((Container) c).getComponents();
                for (int i = 0; i < s.length; i++) {
                    traverse(s[i], collection);
                }
            }
        }
    }
}

