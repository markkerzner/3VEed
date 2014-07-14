package org.threeveed.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.mail.Address;
import javax.mail.BodyPart;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Part;
import javax.mail.Session;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmlParser {
    private static final Logger log = LoggerFactory.getLogger(EmlParser.class);
    
    private File emailFile;
    private ArrayList<String> to;
    private Address[] _bcc;
    private Address[] _cc;
    private Address[] _to;
    private Address[] _from;
    private String _subject;
    private Object _content;
    private MimeMessage email;
    private List<String> _attachments;
    private Date _date;
    private Date _sentDate;
    private Map<String, String> attachmentsContent;
    private int attachmentSeq = 0;

    public EmlParser(File emailFile) throws Exception {
        this.emailFile = emailFile;
        _attachments = new ArrayList<>();
        System.setProperty("mail.mime.address.strict", "false");
        System.setProperty("mail.mime.decodeparameters", "true");
        attachmentsContent = new HashMap<>();
        
        parseEmail();
    }

    private void parseEmail() throws Exception {
        java.util.Properties properties = System.getProperties();
        Session session = Session.getDefaultInstance(properties);
        
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(emailFile);
            email = new MimeMessage(session, fis);
            _bcc = email.getRecipients(RecipientType.BCC);
            _cc = email.getRecipients(RecipientType.CC);
            _to = email.getRecipients(RecipientType.TO);
            _from = email.getFrom();
            _subject = email.getSubject();
            _content =  email.getContent();
            _date = email.getReceivedDate();
            _sentDate = email.getSentDate();
            //System.out.println("content type: " + email.getContentType());
            //System.out.println("\nsubject: " + email.getSubject());
            //to = EmailUtil.parseAddressLines(email
             //       .getHeader(Message.RecipientType.TO.toString()));
        } catch (MessagingException e) {
            throw new IllegalStateException("illegal state issue", e);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("file not found issue issue: "
                    + emailFile.getAbsolutePath(), e);
        } catch (Exception e) {
            log.error("Problem parsing eml file", e);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    log.warn("Problem closing the stream", e);
                }
            }
        }
    }
    
    private List<String> getAddressAsList(Address[] address) {
        List<String> result = new ArrayList<String>();
        if(address!=null) {
            for(Address a : address) {
                result.add(a.toString());
            }
        }
        return result;
    }
    
    public List<String> getFrom() {
        return getAddressAsList(_from);
    }
    
    public List<String> getRecepient() {
        return getAddressAsList(_to);
    }
    
    public List<String> getCC() {
        return getAddressAsList(_cc);
    }
    
    public List<String> getBCC() {
        return getAddressAsList(_bcc);
    }
    
    public String getSubject() {
        return _subject;
    }
    
    public Date getDate() {
        return _date;
    }
    
    public String getContent() throws MessagingException, IOException {
        if (_content instanceof String) {
            return _content.toString();
        } 
        
        if (_content instanceof MimeMultipart) {
            MimeMultipart cnt = (MimeMultipart)_content;
            int size = cnt.getCount();
            StringBuilder res = new StringBuilder();
            
            for (int i = 0; i < size; ++i) {
                BodyPart bp = cnt.getBodyPart(i);
                res.append(dumpPart(bp));
            } 
            return res.toString();
        }
        
        return "";
    }
    
    private String dumpPart(Part p) throws MessagingException, IOException {
        
        StringBuilder buf = new StringBuilder();
        if(p.isMimeType("text/plain")) {
            buf.append(p.getContent());
        } else if(p.isMimeType("multipart/*")) {
            Multipart mp = (Multipart)p.getContent();
            int count = mp.getCount();
            for (int i = 0; i < count; i++) {
                BodyPart bp = mp.getBodyPart(i);
                if (bp.isMimeType("text/plain")) {
                    buf.append(dumpPart(bp));
                }
            }
        } else if (p.isMimeType("message/rfc822")) {
            buf.append(dumpPart((Part)p.getContent()));
        } else
            {
            String disp = null;
            try {
                disp = p.getDisposition();                
            } catch (Exception e) {
            }
            
            String filename = "attach-" + (attachmentSeq++);
            try {
                filename = p.getFileName();
            } catch (Exception e) {
                log.error("Problem getting the real attachment name", e);
            }
            
            if (disp == null || disp.equalsIgnoreCase(Part.ATTACHMENT)) {
                log.debug("Adding attachment: " + filename);
                
                _attachments.add(filename);
            }
        }
        return buf.toString();
    }

    /**
     * @return the emailFile
     */
    public File getEmailFile() {
        return emailFile;
    }

    /**
     * @param emailFile
     *            the emailFile to set
     */
    public void setEmailFile(File emailFile) {
        this.emailFile = emailFile;
    }

    /**
     * @param to
     *            the to to set
     */
    public void setTo(ArrayList<String> to) {
        this.to = to;
    }

    /**
     * @return the to
     */
    public List<String> getTo() {
        return getAddressAsList(_to);
    }
    
    public List<String> getAttachmentNames() {
        return _attachments;
    }

    public Date getSentDate() {
        return this._sentDate;
    }
    
    public void saveAttachments() throws MessagingException, IOException {
        if (email.isMimeType("text/*")) {
            // no attachments there - this is just the email itself
        }
        // TODO - why repeat the code?
        if (email.isMimeType("multipart/alternative")) {
            Multipart mp = (Multipart) email.getContent();
            for (int i = 0; i < mp.getCount(); i++) {
                MimeBodyPart bodyPart = (MimeBodyPart) mp.getBodyPart(i);
                String attachmentFileName = bodyPart.getFileName();
                if (attachmentFileName != null) {
                    bodyPart.saveFile(attachmentFileName);
                }
            }
        } else if (email.isMimeType("multipart/*")) {
            Multipart mp = (Multipart) email.getContent();
            for (int i = 0; i < mp.getCount(); i++) {
                MimeBodyPart bodyPart = (MimeBodyPart) mp.getBodyPart(i);
                String attachmentFileName = bodyPart.getFileName();
                if (attachmentFileName != null) {
                    bodyPart.saveFile(attachmentFileName);
                }
            }
        }
    }
    
    public Map<String, String> getAttachmentsContent() {
        return attachmentsContent;
    }
}
