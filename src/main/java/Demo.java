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
    private boolean batchExecutionAllowed = false;
    private boolean hintSpeedUpAllowed = false;

    public Demo(final DB db, boolean useBatchExecution, boolean useHintSpeedUp) {
        System.out.println("Всего необходимо демо-записей: " + fakeRecordsAmountRequired);
        System.out.println("Разрешено генерировать записей на одну запрос: " + recordsPerUpdateAmount);
        System.out.println("Использовать пакетную выгрузку в БД: " + (useBatchExecution ? "ДА" : "НЕТ"));
        this.db = db;
        this.batchExecutionAllowed = useBatchExecution;
        this.hintSpeedUpAllowed = useHintSpeedUp;
    }

    public int getFakeRecordsAmountRequired() {
        return fakeRecordsAmountRequired;
    }

    public Demo setFakeRecordsAmountRequired(int fakeRecordsAmountRequired) {
        this.fakeRecordsAmountRequired = fakeRecordsAmountRequired;
        return this;
    }

    public boolean insertFakeData(String generationMode) {
        Date startSeconds = new Date();
        System.out.println("Добавление " + generationMode);
        String sqlRequest;
        Date timeStart = new Date();
        float middleSpeed = 0;
        for (int i = 0; i < fakeRecordsAmountRequired; i = i + recordsPerUpdateAmount) {
            if (batchExecutionAllowed) {
                String[] sbqs = getRawsQuery(i, generationMode);
                db.executeRawBatch(sbqs);
                //System.out.println(sbqs[0]);
                float timeDiff = new Date().getTime() / 1000 - timeStart.getTime() / 1000;
                float speed = i / (timeDiff > 0 ? timeDiff : 1);
                middleSpeed = speed;
                System.out.println("Создано записей: " + (i + recordsPerUpdateAmount) + ", скорость " + Math.floor(speed) + " записей в секунду, осталось ~" + (Math.floor(((fakeRecordsAmountRequired - i) / speed) / 60 * 100) / 100) + " минут");
            } else {
                sqlRequest = "INSERT ALL"+(hintSpeedUpAllowed ? " /*+ APPEND PARALLEL(8) INDEX(id) FULL("+generationMode+") */" : "" )+"\n" + getSubQuery(i, generationMode) + "SELECT 1 FROM DUAL";
                db.insert(sqlRequest);
                //System.out.println(sqlRequest);
                float timeDiff = new Date().getTime() / 1000 - timeStart.getTime() / 1000;
                float speed = i / (timeDiff > 0 ? timeDiff : 1);
                middleSpeed = speed;
                System.out.println("Создано записей: " + (i + recordsPerUpdateAmount) + ", скорость " + Math.floor(speed) + " записей в секунду, осталось ~" + (Math.floor(((fakeRecordsAmountRequired - i) / speed) / 60 * 100) / 100) + " минут");
            }
        }
        System.out.println("Сгенерировано " + generationMode);
        System.out.println("Завтрачено времени: " + Math.floor(new Date().getTime()-startSeconds.getTime())+" секунд, средняя скорость "+Math.floor(middleSpeed)+" записей в секунду");
        //db.commit();
        return true;
    }

    private String getSubQuery(int i, String generationMode) {
        String subQuery = "";
        for (int i2 = i; i2 < i + recordsPerUpdateAmount; i2++) {
            switch (generationMode) {
                case "shops":
                    subQuery += generateShopsRow(i2) + "\n";
                    break;
                case "categories":
                    subQuery += generateCategoriesRow(i2) + "\n";
                    break;
                case "links":
                    subQuery += generateLinksRow(i2) + "\n";
                    break;
                case "goods":
                    subQuery += generateGoodsRow(i2) + "\n";
                    break;
            }
        }
        return subQuery;
    }

    private String[] getRawsQuery(int i, String generationMode) {
        String[] subQuery = new String[recordsPerUpdateAmount];
        String hint = hintSpeedUpAllowed ? "/*+ APPEND PARALLEL(4) INDEX(id) FULL("+generationMode+") */ " : "";
        for (int i2 = i; i2 < i + recordsPerUpdateAmount; i2++) {
            switch (generationMode) {
                case "shops":
                    subQuery[i2-i] = "INSERT "+hint+generateShopsRow(i2);
                    break;
                case "categories":
                    subQuery[i2-i] = "INSERT "+hint+generateCategoriesRow(i2);
                    break;
                case "links":
                    subQuery[i2-i] = "INSERT "+hint+generateLinksRow(i2);
                    break;
                case "goods":
                    subQuery[i2-i] = "INSERT "+hint+generateGoodsRow(i2);
                    break;
            }
        }
        return subQuery;
    }

    private String generateShopsRow(int i) {
        String[] generatedWords = generateRandomWords(2);
        int randomStreetTypeInt = random.nextInt(10);
        int randomHouseNumber = random.nextInt(30) + 1;
        return "\tINTO shops (id, name, address) VALUES ('" + i + "', '" + generatedWords[0] + "', '" + generatedWords[1] + " " + (randomStreetTypeInt > 4 ? "city" : "town") + ", " + generatedWords[1] + " " + (randomStreetTypeInt > 6 ? "street" : randomStreetTypeInt > 3 ? "boulevard" : "lane") + ", " + randomHouseNumber + "')";
    }

    private String generateCategoriesRow(int i) {
        return "\tINTO categories (id, parent_cat_id, name) VALUES ('" + i + "', " + (random.nextInt(3) > 1 || i < 10 ? "NULL" : "'" + random.nextInt(i) + "'") + ", '" + generateRandomWords(1)[0] + "')";
    }

    private String generateLinksRow(int i) {
        return "\tINTO links (shop_id, cat_id) VALUES ('" + random.nextInt(fakeRecordsAmountRequired) + "', '" + random.nextInt(fakeRecordsAmountRequired) + "')";
    }

    private String generateGoodsRow(int i) {
        return "\tINTO goods (id, cat_id, name, price, count) VALUES ('" + i + "', '" + random.nextInt(fakeRecordsAmountRequired) + "', '" + generateRandomWords(1)[0] + "', '" + random.nextInt(100000) + "', '" + random.nextInt(1000000) + "')";
    }

    public static String[] generateRandomWords(int numberOfWords) {
        String[] randomStrings = new String[numberOfWords];
        for (int i = 0; i < numberOfWords; i++) {
            char[] word = new char[random.nextInt(8) + 3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
            for (int j = 0; j < word.length; j++) {
                word[j] = (char) ('a' + random.nextInt(26));
            }
            randomStrings[i] = new String(word);
        }
        return randomStrings;
    }
}
