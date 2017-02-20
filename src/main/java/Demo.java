import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.Random;

/**
 * Created by iLeonidze on 17.02.2017.
 */
public class Demo {
    private static Random random = new Random();
    private int fakeRecordsAmountRequired = 0;
    private int recordsPerUpdateAmount = 1000;
    private DB db;

    public Demo(final DB db) {
        System.out.println("Всего необходимо демо-записей: "+fakeRecordsAmountRequired);
        System.out.println("Разрешено генерировать записей на одну запрос: "+ recordsPerUpdateAmount);
        this.db = db;
    }

    public int getFakeRecordsAmountRequired() {
        return fakeRecordsAmountRequired;
    }

    public Demo setFakeRecordsAmountRequired(int fakeRecordsAmountRequired) {
        this.fakeRecordsAmountRequired = fakeRecordsAmountRequired;
        return this;
    }

    public boolean insertFakeData(String generationMode){
        System.out.println("Добавление "+generationMode);
        String sqlRequest;
        Date timeStart = new Date();
        for(int i=0;i<fakeRecordsAmountRequired;i=i+recordsPerUpdateAmount){
            sqlRequest = "INSERT ALL\n" +getSubQuery(i,generationMode)+"SELECT 1 FROM DUAL";
            db.insert(sqlRequest);
            //System.out.println(sqlRequest);
            float timeDiff = new Date().getTime()/1000-timeStart.getTime()/1000;
            float speed = i/(timeDiff>0 ? timeDiff : 1);
            System.out.println("Создано записей: "+(i+recordsPerUpdateAmount)+", скорость "+Math.floor(speed)+" записей в секунду, осталось ~"+(Math.floor(((fakeRecordsAmountRequired-i)/speed)/60*100)/100)+" минут");
        }
        System.out.println("Сгенерировано "+generationMode);
        db.commit();
        return true;
    }

    private String getSubQuery(int i,String generationMode){
        String subQuery = "";
        for(int i2=i;i2<i+recordsPerUpdateAmount;i2++){
            switch (generationMode){
                case "shops":
                    subQuery += generateShopsRow(i2)+"\n";
                    break;
                case "categories":
                    subQuery += generateCategoriesRow(i2)+"\n";
                    break;
                case "links":
                    subQuery += generateLinksRow(i2)+"\n";
                    break;
                case "goods":
                    subQuery += generateGoodsRow(i2)+"\n";
                    break;
            }
        }
        return subQuery;
    }

    private String generateShopsRow(int i){
        String[] generatedWords = generateRandomWords(2);
        int randomStreetTypeInt = random.nextInt(10);
        int randomHouseNumber = random.nextInt(30)+1;
        return "\tINTO shops (id, name, address) VALUES ('"+i+"', '"+generatedWords[0]+"', '"+generatedWords[1]+" "+(randomStreetTypeInt>4 ? "city" : "town")+", "+generatedWords[1]+" "+(randomStreetTypeInt>6 ? "street" : randomStreetTypeInt>3 ? "boulevard" : "lane")+", "+randomHouseNumber+"')";
    }
    private String generateCategoriesRow(int i){
        return "\tINTO categories (id, parent_cat_id, name) VALUES ('"+i+"', "+(random.nextInt(3)>1||i<10 ? "NULL" : "'"+random.nextInt(i)+"'")+", '"+generateRandomWords(1)[0]+"')";
    }
    private String generateLinksRow(int i){
        return "\tINTO links (shop_id, cat_id) VALUES ('"+random.nextInt(fakeRecordsAmountRequired)+"', '"+random.nextInt(fakeRecordsAmountRequired)+"')";
    }
    private String generateGoodsRow(int i){
        return "\tINTO goods (id, cat_id, name, price, count) VALUES ('"+i+"', '"+random.nextInt(fakeRecordsAmountRequired)+"', '"+generateRandomWords(1)[0]+"', '"+random.nextInt(100000)+"', '"+random.nextInt(1000000)+"')";
    }

    public static String[] generateRandomWords(int numberOfWords) {
        String[] randomStrings = new String[numberOfWords];
        for(int i = 0; i < numberOfWords; i++)
        {
            char[] word = new char[random.nextInt(8)+3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
            for(int j = 0; j < word.length; j++)
            {
                word[j] = (char)('a' + random.nextInt(26));
            }
            randomStrings[i] = new String(word);
        }
        return randomStrings;
    }
}
