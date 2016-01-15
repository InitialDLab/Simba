<?php
use yii\widgets\ActiveForm;

$form = ActiveForm::begin(['options' => ['enctype' => 'multipart/form-data']]); ?>

<?= $form->field($model, 'file')->fileInput() ?>
<button class="btn btn-lg btn-success">Submit</button>
<?php ActiveForm::end(); ?>
