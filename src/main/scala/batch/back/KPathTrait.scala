package batch.back

import model.back.{ODModel, PathModel}

trait KPathTrait extends Serializable {

  def compute(ODModel: ODModel): List[PathModel]
}
