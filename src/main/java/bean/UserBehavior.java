package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: Gm
 * @Date: 2021/8/10 11:29
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
